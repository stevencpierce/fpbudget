// Cross-platform dialogs. React Native Web's Alert.alert is a NO-OP, so on
// web (the /app browser preview) every confirm/notice must go through
// window.confirm / window.alert instead — otherwise logout, delete-line,
// and the estimated-edit override would all silently do nothing.
import { Alert, Platform } from "react-native";

export function confirmDialog(
  title: string,
  message: string,
  confirmText: string,
  onConfirm: () => void,
  destructive = false
): void {
  if (Platform.OS === "web") {
    if (typeof window !== "undefined" && window.confirm(`${title}\n\n${message}`)) {
      onConfirm();
    }
    return;
  }
  Alert.alert(title, message, [
    { text: "Cancel", style: "cancel" },
    {
      text: confirmText,
      style: destructive ? "destructive" : "default",
      onPress: onConfirm,
    },
  ]);
}

export function notify(title: string, message: string): void {
  if (Platform.OS === "web") {
    if (typeof window !== "undefined") window.alert(`${title}\n\n${message}`);
    return;
  }
  Alert.alert(title, message);
}
