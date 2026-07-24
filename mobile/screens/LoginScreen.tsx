import React, { useEffect, useState } from "react";
import {
  ActivityIndicator,
  KeyboardAvoidingView,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";

import { ApiError, getServer, login, setServer } from "../lib/api";
import { APP_DISPLAY_NAME } from "../lib/config";
import { colors, spacing } from "../lib/theme";
import { LoginResponse } from "../lib/types";

interface Props {
  onLoggedIn: (out: LoginResponse) => void;
}

export default function LoginScreen({ onLoggedIn }: Props) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showServer, setShowServer] = useState(false);
  const [server, setServerText] = useState("");

  useEffect(() => {
    getServer().then(setServerText);
  }, []);

  const submit = async () => {
    if (busy) return;
    setError(null);
    if (!email.trim() || !password) {
      setError("Enter your email and password.");
      return;
    }
    setBusy(true);
    try {
      if (server.trim()) await setServer(server.trim());
      const out = await login(email.trim(), password);
      onLoggedIn(out);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not reach the server.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <KeyboardAvoidingView
      style={styles.root}
      behavior={Platform.OS === "ios" ? "padding" : undefined}
    >
      <ScrollView
        contentContainerStyle={styles.scroll}
        keyboardShouldPersistTaps="handled"
      >
        <Text style={styles.logo}>{APP_DISPLAY_NAME}</Text>
        <Text style={styles.tagline}>Production docs & budgets</Text>

        <View style={styles.card}>
          <Text style={styles.label}>Email</Text>
          <TextInput
            style={styles.input}
            value={email}
            onChangeText={setEmail}
            autoCapitalize="none"
            autoCorrect={false}
            keyboardType="email-address"
            textContentType="username"
            placeholder="you@example.com"
            placeholderTextColor={colors.textDim}
            editable={!busy}
          />
          <Text style={styles.label}>Password</Text>
          <TextInput
            style={styles.input}
            value={password}
            onChangeText={setPassword}
            secureTextEntry
            textContentType="password"
            placeholder="••••••••"
            placeholderTextColor={colors.textDim}
            editable={!busy}
            onSubmitEditing={submit}
          />

          {error ? <Text style={styles.error}>{error}</Text> : null}

          <Pressable
            style={({ pressed }) => [styles.button, pressed && styles.pressed]}
            onPress={submit}
            disabled={busy}
          >
            {busy ? (
              <ActivityIndicator color={colors.accentText} />
            ) : (
              <Text style={styles.buttonText}>Log in</Text>
            )}
          </Pressable>
        </View>

        <Pressable onPress={() => setShowServer(!showServer)}>
          <Text style={styles.serverLink}>
            {showServer ? "Hide server settings" : "Server settings"}
          </Text>
        </Pressable>
        {showServer ? (
          <TextInput
            style={[styles.input, styles.serverInput]}
            value={server}
            onChangeText={setServerText}
            autoCapitalize="none"
            autoCorrect={false}
            keyboardType="url"
            placeholder="https://fp-budget.onrender.com"
            placeholderTextColor={colors.textDim}
          />
        ) : null}
      </ScrollView>
    </KeyboardAvoidingView>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: colors.bg },
  scroll: {
    flexGrow: 1,
    justifyContent: "center",
    padding: spacing.lg,
  },
  logo: {
    color: colors.text,
    fontSize: 34,
    fontWeight: "800",
    textAlign: "center",
  },
  tagline: {
    color: colors.textDim,
    textAlign: "center",
    marginTop: spacing.xs,
    marginBottom: spacing.xl,
  },
  card: {
    backgroundColor: colors.card,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 16,
    padding: spacing.lg,
  },
  label: {
    color: colors.textDim,
    fontSize: 13,
    marginBottom: spacing.xs,
    marginTop: spacing.sm,
  },
  input: {
    backgroundColor: colors.inputBg,
    borderColor: colors.cardBorder,
    borderWidth: 1,
    borderRadius: 10,
    color: colors.text,
    fontSize: 16, // ≥16 stops iOS zoom-on-focus
    paddingHorizontal: spacing.md,
    paddingVertical: 12,
  },
  error: { color: colors.danger, marginTop: spacing.md },
  button: {
    backgroundColor: colors.accent,
    borderRadius: 10,
    paddingVertical: 14,
    alignItems: "center",
    marginTop: spacing.lg,
  },
  pressed: { opacity: 0.8 },
  buttonText: { color: colors.accentText, fontSize: 17, fontWeight: "700" },
  serverLink: {
    color: colors.textDim,
    textAlign: "center",
    marginTop: spacing.xl,
    textDecorationLine: "underline",
  },
  serverInput: { marginTop: spacing.sm },
});
