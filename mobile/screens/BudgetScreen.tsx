import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";

import LineEditorModal from "../components/LineEditorModal";
import { ApiError, fetchBudgetSummary } from "../lib/api";
import { money, moneyCompact } from "../lib/format";
import { colors, spacing } from "../lib/theme";
import {
  ApiProject,
  BudgetInfo,
  BudgetLineData,
  BudgetSummary,
} from "../lib/types";

interface Props {
  project: ApiProject;
  budget: BudgetInfo;
  onBack: () => void;
}

export default function BudgetScreen({ project, budget, onBack }: Props) {
  const [summary, setSummary] = useState<BudgetSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<number>>(new Set());
  const [editing, setEditing] = useState<BudgetLineData | null>(null);

  const canEdit = project.role === "owner" || project.role === "editor";

  const load = useCallback(async () => {
    setError(null);
    try {
      setSummary(await fetchBudgetSummary(project.id, budget.id));
    } catch (e) {
      setError(
        e instanceof ApiError ? e.message : "Could not load the budget."
      );
    }
  }, [project.id, budget.id]);

  useEffect(() => {
    load();
  }, [load]);

  // Group lines by COA section (thousands bucket matches the top sheet).
  const linesBySection = useMemo(() => {
    const map = new Map<number, BudgetLineData[]>();
    for (const ln of summary?.lines || []) {
      const sec = Math.floor(ln.account_code / 1000) * 1000;
      const arr = map.get(sec) || [];
      arr.push(ln);
      map.set(sec, arr);
    }
    return map;
  }, [summary]);

  const toggle = (code: number) =>
    setExpanded((s) => {
      const n = new Set(s);
      if (n.has(code)) n.delete(code);
      else n.add(code);
      return n;
    });

  const hasActuals = (summary?.totals.grand_total_actual || 0) !== 0;

  return (
    <View style={styles.root}>
      <View style={styles.header}>
        <Pressable onPress={onBack} hitSlop={12}>
          <Text style={styles.back}>‹ Budgets</Text>
        </Pressable>
      </View>
      <Text style={styles.title} numberOfLines={1}>
        {budget.name}
      </Text>
      <Text style={styles.mode}>{budget.mode_label}</Text>

      {summary === null && !error ? (
        <ActivityIndicator
          color={colors.accent}
          size="large"
          style={{ marginTop: spacing.xl }}
        />
      ) : error ? (
        <View>
          <Text style={styles.error}>{error}</Text>
          <Pressable onPress={load}>
            <Text style={styles.retryLink}>Try again</Text>
          </Pressable>
        </View>
      ) : summary ? (
        <ScrollView contentContainerStyle={{ paddingBottom: spacing.xl }}>
          <View style={styles.totalCard}>
            <Text style={styles.totalLabel}>Grand total</Text>
            <Text style={styles.totalValue}>
              {money(summary.totals.grand_total_estimated)}
            </Text>
            {hasActuals ? (
              <Text style={styles.totalActual}>
                Actual {money(summary.totals.grand_total_actual)} · Variance{" "}
                {money(summary.totals.grand_variance)}
              </Text>
            ) : null}
            {summary.budget.target_budget ? (
              <Text style={styles.totalActual}>
                Target {moneyCompact(summary.budget.target_budget)}
              </Text>
            ) : null}
          </View>

          {summary.sections.map((sec) => {
            const open = expanded.has(sec.code);
            const secLines = linesBySection.get(sec.code) || [];
            return (
              <View key={sec.code} style={styles.section}>
                <Pressable
                  style={({ pressed }) => [
                    styles.sectionHead,
                    pressed && styles.pressed,
                  ]}
                  onPress={() => toggle(sec.code)}
                >
                  <Text style={styles.sectionChevron}>
                    {open ? "▾" : "▸"}
                  </Text>
                  <View style={{ flex: 1 }}>
                    <Text style={styles.sectionName} numberOfLines={1}>
                      {sec.code} · {sec.account}
                    </Text>
                    {hasActuals && sec.actual ? (
                      <Text style={styles.sectionActual}>
                        actual {money(sec.actual)}
                      </Text>
                    ) : null}
                  </View>
                  <Text style={styles.sectionTotal}>
                    {money(sec.estimated)}
                  </Text>
                </Pressable>
                {open
                  ? secLines.map((ln) => (
                      <Pressable
                        key={ln.id}
                        style={({ pressed }) => [
                          styles.lineRow,
                          pressed && styles.pressed,
                        ]}
                        onPress={() => setEditing(ln)}
                      >
                        <View style={{ flex: 1 }}>
                          <Text style={styles.lineDesc} numberOfLines={1}>
                            {ln.description || ln.account_name}
                          </Text>
                          {ln.is_labor ? (
                            <Text style={styles.lineMeta} numberOfLines={1}>
                              {ln.days ?? 0}d × {money(ln.rate)}
                              {ln.fringe_type ? ` · ${ln.fringe_type}` : ""}
                            </Text>
                          ) : null}
                        </View>
                        <Text style={styles.lineTotal}>{money(ln.total)}</Text>
                      </Pressable>
                    ))
                  : null}
              </View>
            );
          })}

          <View style={styles.autoBlock}>
            {summary.totals.workers_comp_amount ? (
              <Text style={styles.autoLine}>
                Workers' Comp {money(summary.totals.workers_comp_amount)}
              </Text>
            ) : null}
            {summary.totals.payroll_fee_amount ? (
              <Text style={styles.autoLine}>
                Payroll Service Fee {money(summary.totals.payroll_fee_amount)}
              </Text>
            ) : null}
            {summary.totals.production_insurance_amount ? (
              <Text style={styles.autoLine}>
                Production Insurance{" "}
                {money(summary.totals.production_insurance_amount)}
              </Text>
            ) : null}
            {summary.totals.company_fee &&
            !summary.totals.company_fee_dispersed ? (
              <Text style={styles.autoLine}>
                Company Fee {money(summary.totals.company_fee)}
              </Text>
            ) : null}
          </View>
        </ScrollView>
      ) : null}

      <LineEditorModal
        projectId={project.id}
        budgetId={budget.id}
        line={editing}
        canEdit={canEdit}
        onClose={() => setEditing(null)}
        onSaved={load}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.bg, padding: spacing.md },
  header: { flexDirection: "row", marginBottom: spacing.sm },
  back: { color: colors.accent, fontSize: 17 },
  title: { color: colors.text, fontSize: 24, fontWeight: "800" },
  mode: { color: colors.textDim, fontSize: 14, marginBottom: spacing.md },
  error: { color: colors.danger, marginTop: spacing.lg },
  retryLink: { color: colors.accent, marginTop: spacing.sm, fontSize: 16 },
  totalCard: {
    backgroundColor: colors.card,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 16,
    padding: spacing.lg,
    marginBottom: spacing.md,
  },
  totalLabel: {
    color: colors.textDim,
    fontSize: 13,
    textTransform: "uppercase",
    letterSpacing: 1,
  },
  totalValue: {
    color: colors.text,
    fontSize: 32,
    fontWeight: "800",
    marginTop: 2,
  },
  totalActual: { color: colors.textDim, fontSize: 14, marginTop: 4 },
  section: {
    backgroundColor: colors.card,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 12,
    marginBottom: spacing.sm,
    overflow: "hidden",
  },
  sectionHead: {
    flexDirection: "row",
    alignItems: "center",
    padding: spacing.md,
  },
  pressed: { opacity: 0.7 },
  sectionChevron: {
    color: colors.textDim,
    fontSize: 14,
    marginRight: spacing.sm,
  },
  sectionName: { color: colors.text, fontSize: 15, fontWeight: "700" },
  sectionActual: { color: colors.textDim, fontSize: 12, marginTop: 2 },
  sectionTotal: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "700",
    marginLeft: spacing.sm,
  },
  lineRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 10,
    paddingLeft: spacing.lg + spacing.sm,
    paddingRight: spacing.md,
    borderTopColor: colors.cardBorder,
    borderTopWidth: StyleSheet.hairlineWidth,
  },
  lineDesc: { color: colors.text, fontSize: 14 },
  lineMeta: { color: colors.textDim, fontSize: 12, marginTop: 2 },
  lineTotal: { color: colors.textDim, fontSize: 14, marginLeft: spacing.sm },
  autoBlock: { marginTop: spacing.sm, paddingHorizontal: spacing.xs },
  autoLine: { color: colors.textDim, fontSize: 13, marginTop: 4 },
});
