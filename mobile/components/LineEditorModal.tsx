import React, { useEffect, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  KeyboardAvoidingView,
  Modal,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";

import { ApiError, deleteLine, saveLine } from "../lib/api";
import { money } from "../lib/format";
import { colors, spacing } from "../lib/theme";
import { BudgetLineData } from "../lib/types";

interface Props {
  projectId: number;
  budgetId: number;
  line: BudgetLineData | null; // null = hidden
  canEdit: boolean;
  onClose: () => void;
  onSaved: () => void; // parent refetches the summary
}

function numOrUndef(s: string): number | undefined {
  const t = s.trim();
  if (t === "") return undefined;
  const n = Number(t.replace(/[$,\s]/g, ""));
  return Number.isFinite(n) ? n : undefined;
}

export default function LineEditorModal({
  projectId,
  budgetId,
  line,
  canEdit,
  onClose,
  onSaved,
}: Props) {
  const [description, setDescription] = useState("");
  const [quantity, setQuantity] = useState("");
  const [days, setDays] = useState("");
  const [rate, setRate] = useState("");
  const [estOt, setEstOt] = useState("");
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!line) return;
    setDescription(line.description || "");
    setQuantity(line.quantity !== null ? String(line.quantity) : "");
    setDays(line.days !== null ? String(line.days) : "");
    setRate(line.rate !== null ? String(line.rate) : "");
    setEstOt(line.est_ot ? String(line.est_ot) : "");
    setNote(line.note || "");
    setError(null);
    setBusy(false);
  }, [line]);

  if (!line) return null;

  const buildPayload = (override: boolean) => {
    const p: Record<string, unknown> = {
      id: line.id,
      description,
      note: note || null,
    };
    // Only send numeric fields the user can actually see/edit here, and
    // only when they parse — never clobber a field with garbage.
    const q = numOrUndef(quantity);
    const d = numOrUndef(days);
    const r = numOrUndef(rate);
    const ot = numOrUndef(estOt);
    if (!line.is_labor && q !== undefined) p.quantity = q;
    if (d !== undefined) p.days = d;
    if (r !== undefined) p.rate = r;
    if (line.is_labor && ot !== undefined) p.est_ot = ot;
    if (override) p.override_estimated = true;
    return p;
  };

  const doSave = async (override: boolean) => {
    setBusy(true);
    setError(null);
    try {
      await saveLine(projectId, budgetId, buildPayload(override));
      onSaved();
      onClose();
    } catch (e) {
      if (e instanceof ApiError && e.status === 409 && e.body) {
        if (e.body.estimated_protected) {
          setBusy(false);
          Alert.alert(
            "Editing the Estimated budget",
            "A Working budget exists — this change affects Estimated only. Continue?",
            [
              { text: "Cancel", style: "cancel" },
              { text: "Edit Estimated", onPress: () => doSave(true) },
            ]
          );
          return;
        }
        if (e.body.schedule_conflict) {
          setError(
            "This line has schedule days attached — reduce its quantity on the website, where you can pick which schedule instances to remove."
          );
          setBusy(false);
          return;
        }
      }
      setError(e instanceof ApiError ? e.message : "Save failed — try again.");
      setBusy(false);
    }
  };

  const doDelete = () => {
    Alert.alert(
      "Delete this line?",
      `"${line.description || line.account_name}" will be removed from the budget.`,
      [
        { text: "Cancel", style: "cancel" },
        {
          text: "Delete",
          style: "destructive",
          onPress: async () => {
            setBusy(true);
            try {
              await deleteLine(projectId, budgetId, line.id);
              onSaved();
              onClose();
            } catch (e) {
              setError(
                e instanceof ApiError ? e.message : "Delete failed — try again."
              );
              setBusy(false);
            }
          },
        },
      ]
    );
  };

  const field = (
    label: string,
    value: string,
    setter: (s: string) => void,
    keyboard: "default" | "decimal-pad" = "decimal-pad"
  ) => (
    <View style={styles.fieldWrap}>
      <Text style={styles.label}>{label}</Text>
      <TextInput
        style={styles.input}
        value={value}
        onChangeText={setter}
        editable={canEdit && !busy}
        keyboardType={keyboard}
        placeholderTextColor={colors.textDim}
      />
    </View>
  );

  return (
    <Modal visible transparent animationType="slide" onRequestClose={onClose}>
      <KeyboardAvoidingView
        style={styles.backdrop}
        behavior={Platform.OS === "ios" ? "padding" : undefined}
      >
        <View style={styles.sheet}>
          <ScrollView keyboardShouldPersistTaps="handled">
            <Text style={styles.acct}>
              {line.account_code} · {line.account_name}
            </Text>
            <Text style={styles.total}>{money(line.total)}</Text>
            {line.is_labor ? (
              <Text style={styles.breakdown}>
                {money(line.subtotal)} wages
                {line.fringe_amount
                  ? ` + ${money(line.fringe_amount)} fringe`
                  : ""}
                {line.agent_amount
                  ? ` + ${money(line.agent_amount)} agent`
                  : ""}
              </Text>
            ) : null}
            {line.line_tag ? (
              <Text style={styles.autoNote}>
                Auto-calculated line ({line.line_tag}) — editing it here turns
                off auto-sync for this line.
              </Text>
            ) : null}
            {line.use_schedule ? (
              <Text style={styles.autoNote}>
                Days come from the schedule; day edits here may be
                recalculated.
              </Text>
            ) : null}

            <View style={styles.fieldWrap}>
              <Text style={styles.label}>Description</Text>
              <TextInput
                style={styles.input}
                value={description}
                onChangeText={setDescription}
                editable={canEdit && !busy}
                placeholderTextColor={colors.textDim}
              />
            </View>

            <View style={styles.row}>
              {!line.is_labor ? field("Qty", quantity, setQuantity) : null}
              {field(line.is_labor ? "Days" : "Days/Units", days, setDays)}
              {field("Rate", rate, setRate)}
            </View>
            {line.is_labor ? (
              <View style={styles.row}>
                {field("Est. OT ($)", estOt, setEstOt)}
                <View style={styles.fieldWrap}>
                  <Text style={styles.label}>Fringe</Text>
                  <Text style={styles.readonly}>
                    {line.fringe_type || "—"}
                  </Text>
                </View>
                <View style={styles.fieldWrap}>
                  <Text style={styles.label}>Rate type</Text>
                  <Text style={styles.readonly}>{line.rate_type || "—"}</Text>
                </View>
              </View>
            ) : null}

            <View style={styles.fieldWrap}>
              <Text style={styles.label}>Note</Text>
              <TextInput
                style={styles.input}
                value={note}
                onChangeText={setNote}
                editable={canEdit && !busy}
                placeholderTextColor={colors.textDim}
              />
            </View>

            {error ? <Text style={styles.error}>{error}</Text> : null}

            {canEdit ? (
              <Pressable
                style={({ pressed }) => [
                  styles.saveBtn,
                  pressed && styles.pressed,
                ]}
                onPress={() => doSave(false)}
                disabled={busy}
              >
                {busy ? (
                  <ActivityIndicator color={colors.accentText} />
                ) : (
                  <Text style={styles.saveText}>Save</Text>
                )}
              </Pressable>
            ) : (
              <Text style={styles.viewOnly}>
                View only — you don't have edit access on this project.
              </Text>
            )}

            <View style={styles.footerRow}>
              <Pressable onPress={onClose} hitSlop={8} disabled={busy}>
                <Text style={styles.cancel}>Close</Text>
              </Pressable>
              {canEdit ? (
                <Pressable onPress={doDelete} hitSlop={8} disabled={busy}>
                  <Text style={styles.delete}>Delete line</Text>
                </Pressable>
              ) : null}
            </View>
          </ScrollView>
        </View>
      </KeyboardAvoidingView>
    </Modal>
  );
}

const styles = StyleSheet.create({
  backdrop: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.6)",
    justifyContent: "flex-end",
  },
  sheet: {
    backgroundColor: colors.card,
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    padding: spacing.lg,
    maxHeight: "88%",
  },
  acct: { color: colors.textDim, fontSize: 13 },
  total: {
    color: colors.text,
    fontSize: 28,
    fontWeight: "800",
    marginTop: 2,
  },
  breakdown: { color: colors.textDim, fontSize: 13, marginTop: 2 },
  autoNote: {
    color: colors.warning,
    fontSize: 13,
    marginTop: spacing.sm,
    lineHeight: 18,
  },
  row: { flexDirection: "row", gap: spacing.sm },
  fieldWrap: { flex: 1, marginTop: spacing.md },
  label: { color: colors.textDim, fontSize: 13, marginBottom: spacing.xs },
  input: {
    backgroundColor: colors.inputBg,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 10,
    color: colors.text,
    fontSize: 16,
    paddingHorizontal: spacing.md,
    paddingVertical: 10,
  },
  readonly: {
    color: colors.textDim,
    fontSize: 16,
    paddingVertical: 10,
  },
  error: { color: colors.danger, marginTop: spacing.md, lineHeight: 20 },
  saveBtn: {
    backgroundColor: colors.accent,
    borderRadius: 10,
    paddingVertical: 14,
    alignItems: "center",
    marginTop: spacing.lg,
  },
  pressed: { opacity: 0.8 },
  saveText: { color: colors.accentText, fontSize: 17, fontWeight: "700" },
  viewOnly: { color: colors.textDim, marginTop: spacing.lg },
  footerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginTop: spacing.lg,
    marginBottom: spacing.sm,
  },
  cancel: { color: colors.textDim, fontSize: 16 },
  delete: { color: colors.danger, fontSize: 16 },
});
