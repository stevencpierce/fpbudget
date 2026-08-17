import React from "react";
import {
  FlatList,
  Pressable,
  StyleSheet,
  Text,
  View,
} from "react-native";

import { confirmDialog } from "../lib/confirm";
import { colors, spacing } from "../lib/theme";
import { ApiProject, ApiUser } from "../lib/types";

interface Props {
  user: ApiUser;
  projects: ApiProject[];
  refreshing: boolean;
  onRefresh: () => void;
  onOpenProject: (p: ApiProject) => void;
  onLogout: () => void;
}

const ROLE_LABEL: Record<string, string> = {
  owner: "Owner",
  editor: "Editor",
  viewer: "Viewer",
  docs_only: "Docs",
};

export default function ProjectsScreen({
  user,
  projects,
  refreshing,
  onRefresh,
  onOpenProject,
  onLogout,
}: Props) {
  const confirmLogout = () =>
    confirmDialog(
      "Log out?",
      "You'll need your password to log back in.",
      "Log out",
      onLogout,
      true
    );

  return (
    <View style={styles.root}>
      <View style={styles.header}>
        <View style={{ flex: 1 }}>
          <Text style={styles.hello} numberOfLines={1}>
            {user.name || user.email}
          </Text>
          <Text style={styles.role}>{user.display_role}</Text>
        </View>
        <Pressable onPress={confirmLogout} hitSlop={12}>
          <Text style={styles.logout}>Log out</Text>
        </Pressable>
      </View>

      <Text style={styles.title}>Projects</Text>

      <FlatList
        data={projects}
        keyExtractor={(p) => String(p.id)}
        refreshing={refreshing}
        onRefresh={onRefresh}
        contentContainerStyle={{ paddingBottom: spacing.xl }}
        ListEmptyComponent={
          <Text style={styles.empty}>
            No projects yet — ask your line producer to add you to one, then
            pull down to refresh.
          </Text>
        }
        renderItem={({ item }) => (
          <Pressable
            style={({ pressed }) => [styles.card, pressed && styles.pressed]}
            onPress={() => onOpenProject(item)}
          >
            <View style={{ flex: 1 }}>
              <Text style={styles.projectName} numberOfLines={1}>
                {item.name}
              </Text>
              {item.client_name ? (
                <Text style={styles.client} numberOfLines={1}>
                  {item.client_name}
                </Text>
              ) : null}
              {!item.can_upload_docs ? (
                <Text style={styles.noUpload}>
                  Uploads not set up yet (no Dropbox folder)
                </Text>
              ) : null}
            </View>
            <Text style={styles.roleBadge}>
              {ROLE_LABEL[item.role] || item.role}
            </Text>
            <Text style={styles.chevron}>›</Text>
          </Pressable>
        )}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.bg, padding: spacing.md },
  header: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: spacing.lg,
  },
  hello: { color: colors.text, fontSize: 16, fontWeight: "600" },
  role: { color: colors.textDim, fontSize: 13, marginTop: 2 },
  logout: { color: colors.danger, fontSize: 15 },
  title: {
    color: colors.text,
    fontSize: 28,
    fontWeight: "800",
    marginBottom: spacing.md,
  },
  empty: { color: colors.textDim, marginTop: spacing.xl, lineHeight: 20 },
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
  projectName: { color: colors.text, fontSize: 17, fontWeight: "700" },
  client: { color: colors.textDim, fontSize: 14, marginTop: 2 },
  noUpload: { color: colors.warning, fontSize: 12, marginTop: 4 },
  roleBadge: {
    color: colors.textDim,
    fontSize: 12,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 8,
    paddingHorizontal: 8,
    paddingVertical: 3,
    overflow: "hidden",
    marginLeft: spacing.sm,
  },
  chevron: { color: colors.textDim, fontSize: 24, marginLeft: spacing.sm },
});
