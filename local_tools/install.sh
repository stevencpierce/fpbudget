#!/bin/zsh
# FP Document Analyzer — Finder companion installer (macOS).
#
# What it does:
#   1. Installs fp_file_it.py to a stable location that survives you moving
#      or deleting this repo.
#   2. Adds a `fp-file-it` command for the terminal (~/.local/bin).
#   3. Runs the one-time credential setup (password → macOS Keychain).
#   4. Builds a Finder right-click Quick Action ("Analyze & File") so you
#      can select files in Finder → Quick Actions → Analyze & File.
#
# Re-running it is safe — it overwrites cleanly.
#
# Usage:  ./install.sh
set -e

if [[ "$(uname)" != "Darwin" ]]; then
  echo "This installer is for macOS. On Linux/Windows, run fp_file_it.py directly"
  echo "(see README.md) — the terminal usage works anywhere with Python 3.8+."
  exit 1
fi

SRC_DIR="${0:A:h}"                       # directory this script lives in
SRC="$SRC_DIR/fp_file_it.py"
if [[ ! -f "$SRC" ]]; then
  echo "✕ Can't find fp_file_it.py next to this installer ($SRC)."
  exit 1
fi

PYTHON="$(command -v python3 || true)"
if [[ -z "$PYTHON" ]]; then
  echo "✕ python3 not found. Install it first:  xcode-select --install"
  exit 1
fi
echo "• Using Python: $PYTHON ($($PYTHON --version 2>&1))"

# ── 1. Stable install location ───────────────────────────────────────────────
APP_DIR="$HOME/Library/Application Support/FPBudget"
DEST="$APP_DIR/fp_file_it.py"
mkdir -p "$APP_DIR"
cp "$SRC" "$DEST"
chmod +x "$DEST"
echo "• Installed script → $DEST"

# ── 2. Terminal command: fp-file-it ──────────────────────────────────────────
BIN_DIR="$HOME/.local/bin"
mkdir -p "$BIN_DIR"
cat > "$BIN_DIR/fp-file-it" <<SHIM
#!/bin/zsh
exec "$PYTHON" "$DEST" "\$@"
SHIM
chmod +x "$BIN_DIR/fp-file-it"
echo "• Installed command → $BIN_DIR/fp-file-it"
if [[ ":$PATH:" != *":$BIN_DIR:"* ]]; then
  echo "  (add ~/.local/bin to your PATH to use 'fp-file-it' directly:"
  echo "     echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.zshrc )"
fi

# ── 3. Credential setup ──────────────────────────────────────────────────────
echo
echo "── Account setup ─────────────────────────────────────────────"
"$PYTHON" "$DEST" --setup
echo

# ── 4. Finder Quick Action ("Analyze & File") ────────────────────────────────
SERVICE_DIR="$HOME/Library/Services/Analyze & File.workflow/Contents"
mkdir -p "$SERVICE_DIR"

cat > "$SERVICE_DIR/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>NSServices</key>
    <array>
        <dict>
            <key>NSMenuItem</key>
            <dict>
                <key>default</key>
                <string>Analyze &amp; File</string>
            </dict>
            <key>NSMessage</key>
            <string>runWorkflowAsService</string>
            <key>NSRequiredContext</key>
            <dict>
                <key>NSApplicationIdentifier</key>
                <string>com.apple.finder</string>
            </dict>
            <key>NSSendFileTypes</key>
            <array>
                <string>public.item</string>
            </array>
        </dict>
    </array>
</dict>
</plist>
PLIST

# The shell action's command. Automator passes selected files as arguments.
COMMAND_STRING="\"$PYTHON\" \"$DEST\" --notify \"\$@\""

cat > "$SERVICE_DIR/document.wflow" <<WFLOW
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>AMApplicationBuild</key>
    <string>523</string>
    <key>AMApplicationVersion</key>
    <string>2.10</string>
    <key>AMDocumentVersion</key>
    <string>2</string>
    <key>actions</key>
    <array>
        <dict>
            <key>action</key>
            <dict>
                <key>AMAccepts</key>
                <dict>
                    <key>Container</key>
                    <string>List</string>
                    <key>Optional</key>
                    <true/>
                    <key>Types</key>
                    <array>
                        <string>com.apple.cocoa.string</string>
                    </array>
                </dict>
                <key>AMActionVersion</key>
                <string>2.0.3</string>
                <key>AMApplication</key>
                <array>
                    <string>Automator</string>
                </array>
                <key>AMParameterProperties</key>
                <dict>
                    <key>COMMAND_STRING</key>
                    <dict/>
                    <key>CheckedForUserDefaultShell</key>
                    <dict/>
                    <key>inputMethod</key>
                    <dict/>
                    <key>shell</key>
                    <dict/>
                    <key>source</key>
                    <dict/>
                </dict>
                <key>AMProvides</key>
                <dict>
                    <key>Container</key>
                    <string>List</string>
                    <key>Types</key>
                    <array>
                        <string>com.apple.cocoa.string</string>
                    </array>
                </dict>
                <key>ActionBundlePath</key>
                <string>/System/Library/Automator/Run Shell Script.action</string>
                <key>ActionName</key>
                <string>Run Shell Script</string>
                <key>ActionParameters</key>
                <dict>
                    <key>COMMAND_STRING</key>
                    <string>${COMMAND_STRING}</string>
                    <key>CheckedForUserDefaultShell</key>
                    <true/>
                    <key>inputMethod</key>
                    <integer>1</integer>
                    <key>shell</key>
                    <string>/bin/zsh</string>
                    <key>source</key>
                    <string></string>
                </dict>
                <key>BundleIdentifier</key>
                <string>com.apple.RunShellScript</string>
                <key>CFBundleVersion</key>
                <string>2.0.3</string>
                <key>CanShowSelectedItemsWhenRun</key>
                <false/>
                <key>CanShowWhenRun</key>
                <true/>
                <key>Category</key>
                <array>
                    <string>AMCategoryUtilities</string>
                </array>
                <key>Class Name</key>
                <string>RunShellScriptAction</string>
                <key>InputUUID</key>
                <string>00000000-0000-0000-0000-000000000001</string>
                <key>Keywords</key>
                <array>
                    <string>Shell</string>
                    <string>Script</string>
                    <string>Command</string>
                    <string>Run</string>
                    <string>Unix</string>
                </array>
                <key>OutputUUID</key>
                <string>00000000-0000-0000-0000-000000000002</string>
                <key>UUID</key>
                <string>00000000-0000-0000-0000-000000000003</string>
                <key>arguments</key>
                <dict>
                    <key>0</key>
                    <dict>
                        <key>default value</key>
                        <integer>0</integer>
                        <key>name</key>
                        <string>inputMethod</string>
                        <key>required</key>
                        <string>0</string>
                        <key>type</key>
                        <string>0</string>
                        <key>uuid</key>
                        <string>0</string>
                    </dict>
                    <key>1</key>
                    <dict>
                        <key>default value</key>
                        <string></string>
                        <key>name</key>
                        <string>source</string>
                        <key>required</key>
                        <string>0</string>
                        <key>type</key>
                        <string>0</string>
                        <key>uuid</key>
                        <string>1</string>
                    </dict>
                    <key>2</key>
                    <dict>
                        <key>default value</key>
                        <false/>
                        <key>name</key>
                        <string>CheckedForUserDefaultShell</string>
                        <key>required</key>
                        <string>0</string>
                        <key>type</key>
                        <string>0</string>
                        <key>uuid</key>
                        <string>2</string>
                    </dict>
                    <key>3</key>
                    <dict>
                        <key>default value</key>
                        <string></string>
                        <key>name</key>
                        <string>COMMAND_STRING</string>
                        <key>required</key>
                        <string>0</string>
                        <key>type</key>
                        <string>0</string>
                        <key>uuid</key>
                        <string>3</string>
                    </dict>
                    <key>4</key>
                    <dict>
                        <key>default value</key>
                        <string>/bin/sh</string>
                        <key>name</key>
                        <string>shell</string>
                        <key>required</key>
                        <string>0</string>
                        <key>type</key>
                        <string>0</string>
                        <key>uuid</key>
                        <string>4</string>
                    </dict>
                </dict>
                <key>isViewVisible</key>
                <integer>1</integer>
                <key>location</key>
                <string>309.000000:253.000000</string>
                <key>nibPath</key>
                <string>/System/Library/Automator/Run Shell Script.action/Contents/Resources/Base.lproj/main.nib</string>
            </dict>
            <key>isViewVisible</key>
            <integer>1</integer>
        </dict>
    </array>
    <key>connectors</key>
    <dict/>
    <key>workflowMetaData</key>
    <dict>
        <key>serviceApplicationBundleID</key>
        <string>com.apple.finder</string>
        <key>serviceApplicationPath</key>
        <string>/System/Library/CoreServices/Finder.app</string>
        <key>serviceInputTypeIdentifier</key>
        <string>com.apple.Automator.fileSystemObject</string>
        <key>serviceOutputTypeIdentifier</key>
        <string>com.apple.Automator.nothing</string>
        <key>serviceProcessesInput</key>
        <integer>0</integer>
        <key>workflowTypeIdentifier</key>
        <string>com.apple.Automator.servicesMenu</string>
    </dict>
</dict>
</plist>
WFLOW

# Refresh the Services registry so the menu item appears without a re-login.
/System/Library/CoreServices/pbs -flush >/dev/null 2>&1 || true

echo "• Installed Finder Quick Action → \"Analyze & File\""
echo
echo "✓ Done."
echo
echo "Try it now:"
echo "  Terminal →  fp-file-it --dry-run ~/Downloads/some-receipt.pdf"
echo "  Finder   →  right-click any document → Quick Actions → Analyze & File"
echo
echo "First Finder run may ask for Keychain access — click \"Always Allow\"."
echo "If \"Analyze & File\" doesn't appear yet, open System Settings →"
echo "Keyboard → Keyboard Shortcuts → Services → Files and Folders and tick it"
echo "(or just log out and back in)."
