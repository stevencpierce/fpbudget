import React, { useCallback, useEffect, useState } from "react";
import {
  ActivityIndicator,
  FlatList,
  Pressable,
  StyleSheet,
  Text,
  View,
} from "react-native";

import { ApiError, fetchBudgets } from "../lib/api";
import { colors, spacing } from "../lib/theme";
import { ApiProject, BudgetInfo } from "../lib/types";

interface Props {
  project: ApiProject;
  onBack: () => void;
  onOpenBudget: (b: BudgetInfo) => void;
}

export default function BudgetsScreen({ project, onBack, onOpenBudget }: Props) {
  const [budgets, setBudgets] = useState<BudgetInfo[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async () => {
    setError(null);
    try {
      const out = await fetchBudgets(project.id);
      // Current versions first, then by recency (server pre-sorts by
      // recency; this just floats 'current' to the top).
      out.sort(
        (a, b) =>
          Number(b.version_status === "current") -
          Number(a.version_status === "current")
      );
      setBudgets(out);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not load budgets.");
    }
  }, [project.id]);

  useEffect(() => {
    load();
  }, [load]);

  const onRefresh = async () => {
    setRefreshing(true);
    await load();
    setRefreshing(false);
  };

  return (
    <View style={styles.root}>
      <View style={styles.header}>
        <Pressable onPress={onBack} hitSlop={12}>
          <Text style={styles.back}>‹ {project.name}</Text>
        </Pressable>
      </View>
      <Text style={styles.title}>Budgets</Text>

      {budgets === null && !error ? (
        <ActivityIndicator
          color={colors.accent}
          size="large"
          style={{ marginTop: spacing.xl }}
        />
      ) : error ? (
        <View>
          <Text style={styles.error}>{error}</Text>
          <Pressable onPress={load}>
            <Text style={styles.retry}>Try again</Text>
          </Pressable>
        </View>
      ) : (
        <FlatList
          data={budgets}
          keyExtractor={(b) => String(b.id)}
          refreshing={refreshing}
          onRefresh={onRefresh}
          contentContainerStyle={{ paddingBottom: spacing.xl }}
          ListEmptyComponent={
            <Text style={styles.empty}>No budgets in this project yet.</Text>
          }
          renderItem={({ item }) => (
            <Pressable
              style={({ pressed }) => [styles.card, pressed && styles.pressed]}
              onPress={() => onOpenBudget(item)}
            >
              <View style={{ flex: 1 }}>
                <Text style={styles.name} numberOfLines={1}>
                  {item.name}
                </Text>
                <Text style={styles.meta}>
                  {item.mode_label}
                  {item.version_number ? ` · v${item.version_number}` : ""}
                  {item.version_status !== "current" ? " · superseded" : ""}
                </Text>
              </View>
              <Text
                style={[
                  styles.modeBadge,
                  item.mode_label === "Estimated" && { color: colors.accent },
                  item.mode_label === "Working" && { color: colors.success },
                  item.mode_label === "Actual" && { color: colors.warning },
                ]}
              >
                {item.mode_label}
              </Text>
              <Text style={styles.chevron}>›</Text>
            </Pressable>
          )}
        />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.bg, padding: spacing.md },
  header: { flexDirection: "row", marginBottom: spacing.sm },
  back: { color: colors.accent, fontSize: 17 },
  title: {
    color: colors.text,
    fontSize: 28,
    fontWeight: "800",
    marginBottom: spacing.md,
  },
  error: { color: colors.danger, marginTop: spacing.lg },
  retry: { color: colors.accent, marginTop: spacing.sm, fontSize: 16 },
  empty: { color: colors.textDim, marginTop: spacing.xl },
  card: {
    backgroundColor: colors.card,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 14,
    padding: spacing.md,
    marginBottom: spacing.sm,
    flexDirection: "row",
    alignItems: "center",
  },
  pressed: { opacity: 0.7 },
  name: { color: colors.text, fontSize: 17, fontWeight: "700" },
  meta: { color: colors.textDim, fontSize: 13, marginTop: 3 },
  modeBadge: {
    color: colors.textDim,
    fontSize: 12,
    fontWeight: "700",
    marginLeft: spacing.sm,
  },
  chevron: { color: colors.textDim, fontSize: 24, marginLeft: spacing.sm },
});
