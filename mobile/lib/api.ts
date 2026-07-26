// Thin client for the FPBudget /api/v1 endpoints. Token lives in the OS
// keychain (expo-secure-store); the server URL in AsyncStorage. All calls
// reject with ApiError carrying the server's human-readable message.
import * as SecureStore from "expo-secure-store";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { Platform } from "react-native";

import { DEFAULT_SERVER } from "./config";
import {
  BudgetInfo,
  BudgetSummary,
  LineSavePayload,
  LoginResponse,
  MeResponse,
  RecentUpload,
  UploadResponse,
} from "./types";

const TOKEN_KEY = "fpb.token";
const SERVER_KEY = "fpb.server";

export class ApiError extends Error {
  status: number;
  // Parsed JSON body when the server sent one — structured 409s
  // (estimated_protected / schedule_conflict) ride along here.
  body: Record<string, unknown> | null;
  constructor(
    message: string,
    status: number,
    body: Record<string, unknown> | null = null
  ) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

let _server: string | null = null;
let _token: string | null = null;

export async function getServer(): Promise<string> {
  if (_server) return _server;
  _server = (await AsyncStorage.getItem(SERVER_KEY)) || DEFAULT_SERVER;
  return _server;
}

export async function setServer(url: string): Promise<void> {
  _server = url.replace(/\/+$/, "");
  await AsyncStorage.setItem(SERVER_KEY, _server);
}

export async function getToken(): Promise<string | null> {
  if (_token) return _token;
  _token = await SecureStore.getItemAsync(TOKEN_KEY);
  return _token;
}

async function setToken(token: string | null): Promise<void> {
  _token = token;
  if (token) await SecureStore.setItemAsync(TOKEN_KEY, token);
  else await SecureStore.deleteItemAsync(TOKEN_KEY);
}

async function parseError(res: Response): Promise<ApiError> {
  let msg = `Request failed (${res.status})`;
  let body: Record<string, unknown> | null = null;
  try {
    body = await res.json();
    if (body && typeof body.error === "string") msg = body.error;
    else if (body && typeof body.message === "string") msg = body.message;
  } catch {
    // non-JSON body (proxy page, HTML error) — keep the generic message
  }
  return new ApiError(msg, res.status, body);
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const server = await getServer();
  const token = await getToken();
  const headers: Record<string, string> = {
    Accept: "application/json",
    ...(init.headers as Record<string, string> | undefined),
  };
  if (token) headers["Authorization"] = `Bearer ${token}`;
  const res = await fetch(`${server}${path}`, { ...init, headers });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

export async function login(
  email: string,
  password: string
): Promise<LoginResponse> {
  const device_name =
    Platform.OS === "ios" ? "iPhone/iPad app" : "Android app";
  const out = await request<LoginResponse>("/api/v1/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password, device_name }),
  });
  await setToken(out.token);
  return out;
}

export async function logout(): Promise<void> {
  try {
    await request("/api/v1/auth/logout", { method: "POST" });
  } catch {
    // Token may already be dead — clearing locally is what matters.
  }
  await setToken(null);
}

export async function fetchMe(): Promise<MeResponse> {
  return request<MeResponse>("/api/v1/me");
}

export async function fetchRecent(projectId: number): Promise<RecentUpload[]> {
  const out = await request<{ uploads: RecentUpload[] }>(
    `/api/v1/projects/${projectId}/docs/recent`
  );
  return out.uploads;
}

export async function fetchBudgets(projectId: number): Promise<BudgetInfo[]> {
  const out = await request<{ budgets: BudgetInfo[] }>(
    `/api/v1/projects/${projectId}/budgets`
  );
  return out.budgets;
}

export async function fetchBudgetSummary(
  projectId: number,
  budgetId: number
): Promise<BudgetSummary> {
  return request<BudgetSummary>(
    `/api/v1/projects/${projectId}/budgets/${budgetId}/summary`
  );
}

export async function saveLine(
  projectId: number,
  budgetId: number,
  payload: LineSavePayload
): Promise<void> {
  await request(`/api/v1/projects/${projectId}/budgets/${budgetId}/line`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function deleteLine(
  projectId: number,
  budgetId: number,
  lineId: number
): Promise<void> {
  await request(
    `/api/v1/projects/${projectId}/budgets/${budgetId}/line/${lineId}`,
    { method: "DELETE" }
  );
}

export interface PickedFile {
  uri: string;
  name: string;
  type: string;
}

/** Upload one receipt/document. Uses XHR (not fetch) because React Native's
 * XHR exposes upload progress events. The server runs OCR inside this same
 * request, so onPhase("processing") fires once the bytes are up and the
 * response — including vendor/amount — lands when the analyzer finishes. */
export function uploadReceipt(
  projectId: number,
  file: PickedFile,
  onProgress: (fraction: number) => void,
  onPhase: (phase: "uploading" | "processing") => void
): Promise<UploadResponse> {
  return new Promise(async (resolve, reject) => {
    const server = await getServer();
    const token = await getToken();
    if (!token) {
      reject(new ApiError("Not logged in.", 401));
      return;
    }
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${server}/api/v1/projects/${projectId}/docs/upload`);
    xhr.setRequestHeader("Authorization", `Bearer ${token}`);
    xhr.setRequestHeader("Accept", "application/json");
    xhr.timeout = 180000; // OCR + Dropbox filing can take a while
    onPhase("uploading");
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && e.total > 0) onProgress(e.loaded / e.total);
      if (e.lengthComputable && e.loaded >= e.total) onPhase("processing");
    };
    xhr.onload = () => {
      let body: UploadResponse | null = null;
      try {
        body = JSON.parse(xhr.responseText) as UploadResponse;
      } catch {
        body = null;
      }
      if (body && (xhr.status === 201 || xhr.status === 202)) {
        resolve(body);
      } else {
        const msg =
          (body && (body.error || body.message)) ||
          `Upload failed (${xhr.status})`;
        reject(new ApiError(msg, xhr.status));
      }
    };
    xhr.onerror = () =>
      reject(new ApiError("Network error — check your connection.", 0));
    xhr.ontimeout = () =>
      reject(new ApiError("Timed out — the server took too long.", 0));

    const form = new FormData();
    // React Native's FormData takes {uri, name, type} for files; the DOM
    // typings don't know that shape, hence the cast.
    form.append("file", {
      uri: file.uri,
      name: file.name,
      type: file.type,
    } as unknown as Blob);
    xhr.send(form);
  });
}
