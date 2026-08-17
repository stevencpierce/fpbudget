import React from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import { colors, spacing } from "../lib/theme";
import { ApiProject } from "../lib/types";

interface Props {
  project: ApiProject;
  onBack: () => void;
  onOpenBudgets: () => void;
  onOpenUpload: () => void;
}

export default function ProjectHomeScreen({
  project,
  onBack,
  onOpenBudgets,
  onOpenUpload,
}: Props) {
  return (
    <View style={styles.root}>
      <View style={styles.header}>
        <Pressable onPress={onBack} hitSlop={12}>
          <Text style={styles.back}>‹ Projects</Text>
        </Pressable>
      </View>
      <Text style={styles.title} numberOfLines={2}>
        {project.name}
      </Text>
      {project.client_name ? (
        <Text style={styles.client}>{project.client_name}</Text>
      ) : null}

      <View style={styles.menu}>
        <Pressable
          style={({ pressed }) => [styles.item, pressed && styles.pressed]}
          onPress={onOpenBudgets}
        >
          <Text style={styles.itemEmoji}>💰</Text>
          <View style={{ flex: 1 }}>
            <Text style={styles.itemTitle}>Budgets</Text>
            <Text style={styles.itemSub}>
              Estimated & working — view totals, edit lines
            </Text>
          </View>
          <Text style={styles.chevron}>›</Text>
        </Pressable>

        <Pressable
          style={({ pressed }) => [styles.item, pressed && styles.pressed]}
          onPress={onOpenUpload}
        >
          <Text style={styles.itemEmoji}>🧾</Text>
          <View style={{ flex: 1 }}>
            <Text style={styles.itemTitle}>Upload documents</Text>
            <Text style={styles.itemSub}>
              Receipts & invoices — camera or photo library
            </Text>
          </View>
          <Text style={styles.chevron}>›</Text>
        </Pressable>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.bg, padding: spacing.md },
  header: { flexDirection: "row", marginBottom: spacing.sm },
  back: { color: colors.accent, fontSize: 17 },
  title: { color: colors.text, fontSize: 26, fontWeight: "800" },
  client: { color: colors.textDim, fontSize: 15, marginTop: 2 },
  menu: { marginTop: spacing.xl },
  item: {
    backgroundColor: colors.card,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 16,
    padding: spacing.lg,
    marginBottom: spacing.md,
    flexDirection: "row",
    alignItems: "center",
  },
  pressed: { opacity: 0.7 },
  itemEmoji: { fontSize: 28, marginRight: spacing.md },
  itemTitle: { color: colors.text, fontSize: 18, fontWeight: "700" },
  itemSub: { color: colors.textDim, fontSize: 13, marginTop: 3 },
  chevron: { color: colors.textDim, fontSize: 26, marginLeft: spacing.sm },
});
