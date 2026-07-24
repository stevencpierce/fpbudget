// Shapes returned by the FPBudget /api/v1 endpoints (see routes/api_v1.py
// and MOBILE_APP_PLAN.md — v1 responses may gain fields but never lose them).

export interface ApiUser {
  id: number;
  email: string;
  name: string | null;
  role: string;
  display_role: string;
  is_docs_only: boolean;
}

export interface ApiProject {
  id: number;
  name: string;
  client_name: string | null;
  status: string;
  role: string; // owner | editor | viewer | docs_only
  can_upload_docs: boolean;
}

export interface MeResponse {
  user: ApiUser;
  projects: ApiProject[];
}

export interface LoginResponse {
  token: string;
  user: ApiUser;
}

// POST /api/v1/projects/<pid>/docs/upload — synchronous: OCR runs during
// the request, so vendor/amount/doc_date arrive in this same response.
export interface UploadResponse {
  status: "ok" | "review_dup" | "review" | "error";
  upload_id?: number;
  filed_filename?: string | null;
  new_filename?: string | null;
  doc_type?: string | null;
  confidence?: number | null;
  duplicate?: boolean;
  duplicate_of?: number | null;
  message?: string;
  vendor?: string | null;
  amount?: number | null;
  doc_date?: string | null;
  doc_number?: string | null;
  error?: string;
}

export interface RecentUpload {
  id: number;
  status: string;
  original_filename: string | null;
  filed_filename: string | null;
  vendor: string | null;
  amount: number | null;
  doc_date: string | null;
  is_duplicate: boolean;
  uploaded_at: string | null;
}
