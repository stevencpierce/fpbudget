import React, { useCallback, useEffect, useState } from "react";
import { ActivityIndicator, StyleSheet, View } from "react-native";
import { SafeAreaProvider, SafeAreaView } from "react-native-safe-area-context";
import { StatusBar } from "expo-status-bar";

import { ApiError, fetchMe, getToken, logout } from "./lib/api";
import { colors } from "./lib/theme";
import { ApiProject, MeResponse } from "./lib/types";
import LoginScreen from "./screens/LoginScreen";
import ProjectsScreen from "./screens/ProjectsScreen";
import UploadScreen from "./screens/UploadScreen";

type Phase = "loading" | "login" | "home";

export default function App() {
  const [phase, setPhase] = useState<Phase>("loading");
  const [me, setMe] = useState<MeResponse | null>(null);
  const [activeProject, setActiveProject] = useState<ApiProject | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const loadMe = useCallback(async (): Promise<boolean> => {
    try {
      const out = await fetchMe();
      setMe(out);
      return true;
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) {
        // Token revoked/expired — back to login.
        await logout();
        setMe(null);
        return false;
      }
      // Network hiccup: keep whatever we already had on screen.
      return me !== null;
    }
  }, [me]);

  useEffect(() => {
    (async () => {
      const token = await getToken();
      if (!token) {
        setPhase("login");
        return;
      }
      const ok = await loadMe();
      setPhase(ok ? "home" : "login");
    })();
    // Startup only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const onLoggedIn = async () => {
    setPhase("loading");
    const ok = await loadMe();
    setPhase(ok ? "home" : "login");
  };

  const onLogout = async () => {
    await logout();
    setMe(null);
    setActiveProject(null);
    setPhase("login");
  };

  const onRefresh = async () => {
    setRefreshing(true);
    await loadMe();
    setRefreshing(false);
  };

  let body: React.ReactNode;
  if (phase === "loading") {
    body = (
      <View style={styles.center}>
        <ActivityIndicator size="large" color={colors.accent} />
      </View>
    );
  } else if (phase === "login" || !me) {
    body = <LoginScreen onLoggedIn={onLoggedIn} />;
  } else if (activeProject) {
    body = (
      <UploadScreen
        project={activeProject}
        onBack={() => setActiveProject(null)}
      />
    );
  } else {
    body = (
      <ProjectsScreen
        user={me.user}
        projects={me.projects}
        refreshing={refreshing}
        onRefresh={onRefresh}
        onOpenProject={setActiveProject}
        onLogout={onLogout}
      />
    );
  }

  return (
    <SafeAreaProvider>
      <SafeAreaView style={styles.root} edges={["top", "bottom"]}>
        <StatusBar style="light" />
        {body}
      </SafeAreaView>
    </SafeAreaProvider>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.bg },
  center: {
    flex: 1,
    backgroundColor: colors.bg,
    alignItems: "center",
    justifyContent: "center",
  },
});
