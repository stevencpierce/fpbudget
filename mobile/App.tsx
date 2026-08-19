import React, { useCallback, useEffect, useState } from "react";
import { ActivityIndicator, StyleSheet, View } from "react-native";
import { SafeAreaProvider, SafeAreaView } from "react-native-safe-area-context";
import { StatusBar } from "expo-status-bar";

import { ApiError, fetchMe, getToken, logout } from "./lib/api";
import { colors } from "./lib/theme";
import { ApiProject, BudgetInfo, MeResponse } from "./lib/types";
import BudgetScreen from "./screens/BudgetScreen";
import BudgetsScreen from "./screens/BudgetsScreen";
import LoginScreen from "./screens/LoginScreen";
import ProjectHomeScreen from "./screens/ProjectHomeScreen";
import ProjectsScreen from "./screens/ProjectsScreen";
import UploadScreen from "./screens/UploadScreen";

type Phase = "loading" | "login" | "home";

// Hand-rolled stack — four screens don't justify a navigation library.
type Route =
  | { name: "projectHome"; project: ApiProject }
  | { name: "upload"; project: ApiProject }
  | { name: "budgets"; project: ApiProject }
  | { name: "budget"; project: ApiProject; budget: BudgetInfo };

export default function App() {
  const [phase, setPhase] = useState<Phase>("loading");
  const [me, setMe] = useState<MeResponse | null>(null);
  const [stack, setStack] = useState<Route[]>([]);
  const [refreshing, setRefreshing] = useState(false);
  // Diagnostic notice shown on the login screen. A silent bounce back to
  // login is undebuggable from a user's description (learned 2026-08-18);
  // every failure path must say what happened, with a code to report.
  const [notice, setNotice] = useState<string | null>(null);

  const push = (r: Route) => setStack((s) => [...s, r]);
  const pop = () => setStack((s) => s.slice(0, -1));

  const loadMe = useCallback(
    async (context: "startup" | "login" | "refresh"): Promise<boolean> => {
      try {
        const out = await fetchMe();
        setMe(out);
        return true;
      } catch (e) {
        if (e instanceof ApiError && e.status === 401) {
          // Token revoked/expired — back to login. At startup that's
          // normal (old token); right after login it's a server-side
          // problem worth reporting loudly.
          await logout();
          setMe(null);
          if (context === "login") {
            setNotice(
              "Your password was accepted, but the server then rejected " +
                "the session (code ME-401). Please report this code."
            );
          }
          return false;
        }
        if (me !== null) return true; // network hiccup: keep what we had
        if (context === "login") {
          setNotice(
            e instanceof ApiError
              ? `Signed in, but couldn't load your projects: ${e.message}`
              : "Signed in, but couldn't reach the server after login."
          );
        }
        return false;
      }
    },
    [me]
  );

  useEffect(() => {
    (async () => {
      const token = await getToken();
      if (!token) {
        setPhase("login");
        return;
      }
      const ok = await loadMe("startup");
      setPhase(ok ? "home" : "login");
    })();
    // Startup only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const onLoggedIn = async () => {
    setPhase("loading");
    setNotice(null);
    const ok = await loadMe("login");
    setPhase(ok ? "home" : "login");
  };

  const onLogout = async () => {
    await logout();
    setMe(null);
    setStack([]);
    setPhase("login");
  };

  const openProject = (p: ApiProject) => {
    // Docs-only accounts (or docs-only membership on this project) go
    // straight to upload — mirrors the website's docs_only gate.
    if (me?.user.is_docs_only || p.role === "docs_only") {
      push({ name: "upload", project: p });
    } else {
      push({ name: "projectHome", project: p });
    }
  };

  const onRefresh = async () => {
    setRefreshing(true);
    await loadMe("refresh");
    setRefreshing(false);
  };

  let body: React.ReactNode;
  const top = stack[stack.length - 1];
  if (phase === "loading") {
    body = (
      <View style={styles.center}>
        <ActivityIndicator size="large" color={colors.accent} />
      </View>
    );
  } else if (phase === "login" || !me) {
    body = <LoginScreen onLoggedIn={onLoggedIn} notice={notice} />;
  } else if (!top) {
    body = (
      <ProjectsScreen
        user={me.user}
        projects={me.projects}
        refreshing={refreshing}
        onRefresh={onRefresh}
        onOpenProject={openProject}
        onLogout={onLogout}
      />
    );
  } else if (top.name === "projectHome") {
    body = (
      <ProjectHomeScreen
        project={top.project}
        onBack={pop}
        onOpenBudgets={() => push({ name: "budgets", project: top.project })}
        onOpenUpload={() => push({ name: "upload", project: top.project })}
      />
    );
  } else if (top.name === "upload") {
    body = <UploadScreen project={top.project} onBack={pop} />;
  } else if (top.name === "budgets") {
    body = (
      <BudgetsScreen
        project={top.project}
        onBack={pop}
        onOpenBudget={(b) =>
          push({ name: "budget", project: top.project, budget: b })
        }
      />
    );
  } else {
    body = (
      <BudgetScreen project={top.project} budget={top.budget} onBack={pop} />
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
