import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  FlatList,
  Modal,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import * as ImagePicker from "expo-image-picker";

import {
  ApiError,
  PickedFile,
  fetchPayees,
  fetchRecent,
  uploadReceipt,
} from "../lib/api";
import { notify } from "../lib/confirm";
import { colors, spacing } from "../lib/theme";
import { ApiPayee, ApiProject, RecentUpload, UploadResponse } from "../lib/types";

interface Props {
  project: ApiProject;
  onBack: () => void;
}

type ItemState =
  | "queued"
  | "uploading"
  | "processing"
  | "done"
  | "review"
  | "duplicate"
  | "error";

interface QueueItem {
  key: string;
  file: PickedFile;
  state: ItemState;
  progress: number; // 0..1 while uploading
  message?: string;
  vendor?: string | null;
  amount?: number | null;
  // The payee chosen when the file was queued (Documents Phase 2) — a crew
  // member photographing their own W-9 or invoice tags it on the spot.
  payeeId?: number | null;
  payeeName?: string | null;
}

let _nextKey = 1;

function assetToFile(a: ImagePicker.ImagePickerAsset): PickedFile {
  const guessedExt = a.uri.split(".").pop()?.toLowerCase() || "jpg";
  const name = a.fileName || `photo_${Date.now()}.${guessedExt}`;
  const type =
    a.mimeType || (guessedExt === "png" ? "image/png" : "image/jpeg");
  return { uri: a.uri, name, type };
}

function money(n: number | null | undefined): string | null {
  if (n === null || n === undefined) return null;
  return `$${n.toFixed(2)}`;
}

export default function UploadScreen({ project, onBack }: Props) {
  const [queue, setQueue] = useState<QueueItem[]>([]);
  const [recent, setRecent] = useState<RecentUpload[]>([]);
  const [refreshing, setRefreshing] = useState(false);
  const uploadingRef = useRef(false);
  // Optional Payee picker (Documents Phase 2): the roster from
  // GET /api/v1/payees, this project's people first; the pick applies to
  // every file queued after it until cleared.
  const [payees, setPayees] = useState<ApiPayee[]>([]);
  const [payee, setPayee] = useState<ApiPayee | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [payeeQuery, setPayeeQuery] = useState("");

  useEffect(() => {
    let alive = true;
    fetchPayees(project.id)
      .then((rows) => { if (alive) setPayees(rows); })
      .catch(() => { /* picker stays empty; uploads still work */ });
    return () => { alive = false; };
  }, [project.id]);

  const loadRecent = useCallback(async () => {
    try {
      setRecent(await fetchRecent(project.id));
    } catch {
      // Non-fatal: the queue still shows this session's work.
    }
  }, [project.id]);

  useEffect(() => {
    loadRecent();
  }, [loadRecent]);

  const patchItem = (key: string, patch: Partial<QueueItem>) =>
    setQueue((q) => q.map((it) => (it.key === key ? { ...it, ...patch } : it)));

  const runUpload = async (item: QueueItem) => {
    try {
      const res: UploadResponse = await uploadReceipt(
        project.id,
        item.file,
        (f) => patchItem(item.key, { progress: f }),
        (phase) =>
          patchItem(item.key, {
            state: phase === "processing" ? "processing" : "uploading",
          }),
        item.payeeId
      );
      const state: ItemState =
        res.status === "ok"
          ? "done"
          : res.status === "review"
          ? "review"
          : res.status === "review_dup"
          ? "duplicate"
          : "error";
      patchItem(item.key, {
        state,
        message: res.message,
        vendor: res.vendor,
        amount: res.amount,
      });
      loadRecent();
    } catch (e) {
      patchItem(item.key, {
        state: "error",
        message:
          e instanceof ApiError ? e.message : "Upload failed — try again.",
      });
    } finally {
      // Clearing the flag + the patchItem above re-runs the worker effect,
      // which picks up the next queued item.
      uploadingRef.current = false;
    }
  };

  // One-at-a-time worker: uploads are memory-heavy server-side (OCR runs in
  // the request), so serializing is kinder to the 512 MB Render worker and
  // gives the user one clear progress bar at a time.
  useEffect(() => {
    if (uploadingRef.current) return;
    const next = queue.find((it) => it.state === "queued");
    if (!next) return;
    uploadingRef.current = true;
    patchItem(next.key, { state: "uploading" });
    runUpload(next);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queue]);

  const enqueue = (assets: ImagePicker.ImagePickerAsset[]) => {
    const items: QueueItem[] = assets.map((a) => ({
      key: `q${_nextKey++}`,
      file: assetToFile(a),
      state: "queued",
      progress: 0,
      payeeId: payee ? payee.id : null,
      payeeName: payee ? payee.name : null,
    }));
    setQueue((q) => [...items, ...q]);
  };

  const takePhoto = async () => {
    const perm = await ImagePicker.requestCameraPermissionsAsync();
    if (!perm.granted) {
      notify(
        "Camera access needed",
        "Allow camera access in Settings to photograph receipts."
      );
      return;
    }
    const out = await ImagePicker.launchCameraAsync({ quality: 0.8 });
    if (!out.canceled) enqueue(out.assets);
  };

  const pickFromLibrary = async () => {
    const perm = await ImagePicker.requestMediaLibraryPermissionsAsync();
    if (!perm.granted) {
      notify(
        "Photos access needed",
        "Allow photo library access in Settings to upload receipts."
      );
      return;
    }
    const out = await ImagePicker.launchImageLibraryAsync({
      mediaTypes: ["images"],
      allowsMultipleSelection: true,
      selectionLimit: 10,
      quality: 0.8,
    });
    if (!out.canceled) enqueue(out.assets);
  };

  const retry = (key: string) =>
    patchItem(key, { state: "queued", progress: 0, message: undefined });

  const onRefresh = async () => {
    setRefreshing(true);
    await loadRecent();
    setRefreshing(false);
  };

  const renderQueueItem = (item: QueueItem) => {
    const stateText: Record<ItemState, string> = {
      queued: "Waiting…",
      uploading: `Uploading ${Math.round(item.progress * 100)}%`,
      processing: "Reading receipt…",
      done:
        "Filed ✓" +
        ([item.vendor, money(item.amount)].filter(Boolean).length
          ? "  " + [item.vendor, money(item.amount)].filter(Boolean).join(" · ")
          : ""),
      review: "Uploaded — needs review on the website",
      duplicate: "Possible duplicate — flagged for review",
      error: item.message || "Failed",
    };
    const stateColor: Record<ItemState, string> = {
      queued: colors.textDim,
      uploading: colors.accent,
      processing: colors.accent,
      done: colors.success,
      review: colors.warning,
      duplicate: colors.warning,
      error: colors.danger,
    };
    return (
      <View style={styles.queueItem} key={item.key}>
        <View style={{ flex: 1 }}>
          <Text style={styles.fileName} numberOfLines={1}>
            {item.file.name}
          </Text>
          {item.payeeName ? (
            <Text style={styles.payeeTag} numberOfLines={1}>
              👤 {item.payeeName}
            </Text>
          ) : null}
          <Text style={[styles.stateText, { color: stateColor[item.state] }]}>
            {stateText[item.state]}
          </Text>
          {item.state === "uploading" || item.state === "processing" ? (
            <View style={styles.progressTrack}>
              <View
                style={[
                  styles.progressFill,
                  {
                    width: `${Math.round(
                      (item.state === "processing" ? 1 : item.progress) * 100
                    )}%`,
                  },
                ]}
              />
            </View>
          ) : null}
        </View>
        {item.state === "error" ? (
          <Pressable onPress={() => retry(item.key)} hitSlop={10}>
            <Text style={styles.retry}>Retry</Text>
          </Pressable>
        ) : null}
      </View>
    );
  };

  const recentLine = (u: RecentUpload) => {
    const bits = [u.vendor, money(u.amount), u.doc_date].filter(Boolean);
    return bits.length ? bits.join(" · ") : u.status;
  };

  return (
    <View style={styles.root}>
      <View style={styles.header}>
        <Pressable onPress={onBack} hitSlop={12}>
          <Text style={styles.back}>‹ Projects</Text>
        </Pressable>
      </View>
      <Text style={styles.title} numberOfLines={1}>
        {project.name}
      </Text>

      {project.can_upload_docs ? (
        <View>
          <Pressable
            style={({ pressed }) => [styles.payeeRow, pressed && styles.pressed]}
            onPress={() => setPickerOpen(true)}
            accessibilityLabel="Choose the payee these documents belong to"
          >
            <Text style={styles.payeeLabel}>Payee</Text>
            <Text style={[styles.payeeValue, !payee && styles.payeeEmpty]} numberOfLines={1}>
              {payee ? payee.name : "Optional — whose document is this?"}
            </Text>
            {payee ? (
              <Pressable onPress={() => setPayee(null)} hitSlop={10}>
                <Text style={styles.payeeClear}>✕</Text>
              </Pressable>
            ) : (
              <Text style={styles.payeeClear}>›</Text>
            )}
          </Pressable>
        <View style={styles.buttonRow}>
          <Pressable
            style={({ pressed }) => [styles.bigButton, pressed && styles.pressed]}
            onPress={takePhoto}
          >
            <Text style={styles.bigButtonEmoji}>📷</Text>
            <Text style={styles.bigButtonText}>Take photo</Text>
          </Pressable>
          <Pressable
            style={({ pressed }) => [styles.bigButton, pressed && styles.pressed]}
            onPress={pickFromLibrary}
          >
            <Text style={styles.bigButtonEmoji}>🖼️</Text>
            <Text style={styles.bigButtonText}>From library</Text>
          </Pressable>
        </View>
        </View>
      ) : (
        <View style={styles.notReady}>
          <Text style={styles.notReadyText}>
            This project isn't set up for uploads yet (no Dropbox folder).
            Ask your line producer to finish project setup.
          </Text>
        </View>
      )}

      <Modal visible={pickerOpen} animationType="slide" onRequestClose={() => setPickerOpen(false)}>
        <View style={styles.pickerRoot}>
          <View style={styles.header}>
            <Pressable onPress={() => setPickerOpen(false)} hitSlop={12}>
              <Text style={styles.back}>‹ Back</Text>
            </Pressable>
          </View>
          <Text style={styles.title}>Payee</Text>
          <TextInput
            style={styles.search}
            placeholder="Search people and vendors"
            placeholderTextColor={colors.textDim}
            value={payeeQuery}
            onChangeText={setPayeeQuery}
            autoCorrect={false}
          />
          <FlatList
            data={payees.filter(
              (p) => !payeeQuery || p.name.toLowerCase().includes(payeeQuery.toLowerCase())
            )}
            keyExtractor={(p) => String(p.id)}
            keyboardShouldPersistTaps="handled"
            ListHeaderComponent={
              <Pressable
                style={({ pressed }) => [styles.payeeOption, pressed && styles.pressed]}
                onPress={() => { setPayee(null); setPickerOpen(false); }}
              >
                <Text style={[styles.payeeOptionText, { color: colors.textDim }]}>No payee</Text>
              </Pressable>
            }
            ListEmptyComponent={
              <Text style={styles.emptyRecent}>No matching payee.</Text>
            }
            renderItem={({ item }) => (
              <Pressable
                style={({ pressed }) => [styles.payeeOption, pressed && styles.pressed]}
                onPress={() => { setPayee(item); setPickerOpen(false); }}
              >
                <Text style={styles.payeeOptionText} numberOfLines={1}>{item.name}</Text>
                <Text style={styles.payeeOptionMeta}>
                  {item.kind === "vendor" ? "vendor" : item.kind === "loan_out" ? "loan-out" : ""}
                  {item.on_project ? (item.kind === "person" ? "on this project" : " · on this project") : ""}
                </Text>
              </Pressable>
            )}
          />
        </View>
      </Modal>

      <FlatList
        data={recent}
        keyExtractor={(u) => String(u.id)}
        refreshing={refreshing}
        onRefresh={onRefresh}
        contentContainerStyle={{ paddingBottom: spacing.xl }}
        ListHeaderComponent={
          <View>
            {queue.length ? (
              <View>
                <Text style={styles.section}>This session</Text>
                {queue.map(renderQueueItem)}
              </View>
            ) : null}
            <Text style={styles.section}>Recent uploads</Text>
            {!recent.length ? (
              <Text style={styles.emptyRecent}>
                Nothing yet — your uploads will show here with what the
                receipt reader found.
              </Text>
            ) : null}
          </View>
        }
        renderItem={({ item }) => (
          <View style={styles.recentItem}>
            <View style={{ flex: 1 }}>
              <Text style={styles.fileName} numberOfLines={1}>
                {item.filed_filename || item.original_filename || `#${item.id}`}
              </Text>
              <Text style={styles.recentMeta} numberOfLines={1}>
                {recentLine(item)}
              </Text>
            </View>
            <Text
              style={[
                styles.recentStatus,
                {
                  color:
                    item.status === "done"
                      ? colors.success
                      : item.status === "error"
                      ? colors.danger
                      : colors.warning,
                },
              ]}
            >
              {item.status === "done"
                ? "✓"
                : item.status === "review"
                ? "review"
                : item.status}
            </Text>
          </View>
        )}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.bg, padding: spacing.md },
  header: { flexDirection: "row", marginBottom: spacing.sm },
  back: { color: colors.accent, fontSize: 17 },
  title: {
    color: colors.text,
    fontSize: 24,
    fontWeight: "800",
    marginBottom: spacing.md,
  },
  buttonRow: { flexDirection: "row", gap: spacing.sm },
  payeeRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.sm,
    backgroundColor: colors.card,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 12,
    paddingHorizontal: spacing.md,
    paddingVertical: spacing.sm + 2,
    marginBottom: spacing.sm,
  },
  payeeLabel: {
    color: colors.textDim,
    fontSize: 12,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 1,
  },
  payeeValue: { flex: 1, color: colors.text, fontSize: 15, fontWeight: "600" },
  payeeEmpty: { color: colors.textDim, fontWeight: "400" },
  payeeClear: { color: colors.textDim, fontSize: 16, paddingHorizontal: spacing.xs },
  payeeTag: { color: colors.accent, fontSize: 12, marginTop: 2 },
  pickerRoot: { flex: 1, backgroundColor: colors.bg, padding: spacing.md },
  search: {
    backgroundColor: colors.inputBg,
    color: colors.text,
    borderRadius: 10,
    paddingHorizontal: spacing.md,
    paddingVertical: spacing.sm + 2,
    fontSize: 16,
    marginBottom: spacing.sm,
  },
  payeeOption: {
    paddingVertical: spacing.sm + 4,
    borderBottomColor: colors.cardBorder,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  payeeOptionText: { color: colors.text, fontSize: 16, fontWeight: "600" },
  payeeOptionMeta: { color: colors.textDim, fontSize: 12, marginTop: 2 },
  bigButton: {
    flex: 1,
    backgroundColor: colors.accent,
    borderRadius: 16,
    alignItems: "center",
    paddingVertical: spacing.lg,
  },
  pressed: { opacity: 0.8 },
  bigButtonEmoji: { fontSize: 30, marginBottom: spacing.xs },
  bigButtonText: { color: colors.accentText, fontSize: 16, fontWeight: "700" },
  notReady: {
    backgroundColor: colors.card,
    borderColor: colors.warning,
    borderWidth: 1,
    borderRadius: 14,
    padding: spacing.md,
  },
  notReadyText: { color: colors.warning, lineHeight: 20 },
  section: {
    color: colors.textDim,
    fontSize: 13,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 1,
    marginTop: spacing.lg,
    marginBottom: spacing.sm,
  },
  queueItem: {
    backgroundColor: colors.card,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 12,
    padding: spacing.md,
    marginBottom: spacing.sm,
    flexDirection: "row",
    alignItems: "center",
  },
  fileName: { color: colors.text, fontSize: 15, fontWeight: "600" },
  stateText: { fontSize: 13, marginTop: 3 },
  progressTrack: {
    height: 4,
    backgroundColor: colors.inputBg,
    borderRadius: 2,
    marginTop: spacing.sm,
    overflow: "hidden",
  },
  progressFill: {
    height: 4,
    backgroundColor: colors.accent,
    borderRadius: 2,
  },
  retry: { color: colors.accent, fontSize: 15, marginLeft: spacing.md },
  recentItem: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: spacing.sm,
    borderBottomColor: colors.cardBorder,
    borderBottomWidth: StyleSheet.hairlineWidth,
  },
  recentMeta: { color: colors.textDim, fontSize: 13, marginTop: 2 },
  recentStatus: { fontSize: 14, marginLeft: spacing.md },
  emptyRecent: { color: colors.textDim, lineHeight: 20 },
});
