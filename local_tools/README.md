# FP Document Analyzer — Finder companion

`fp_file_it.py` brings the old standalone Document Analyzer behavior back to
the **Finder level**: run it on any file, anywhere on disk, and the analyzed
copy is filed *right next to the original* — no Dropbox, no FPBudget project,
no database rows. Works for personal documents, taxes, side projects, anything.

```
~/Taxes/2026/scan001.pdf              # before

~/Taxes/2026/                         # after
├── PROCESSED DOCUMENTS/
│   └── 2026-03-12_RECEIPT_Home Depot_142.55.pdf   ← renamed processed copy
└── SOURCE DOCUMENTS/
    └── scan001.pdf                                ← original, archived untouched
```

The heavy lifting (Veryfi OCR, doc-type detection, the naming convention)
happens on the FPBudget server via `POST /api/analyze`, which is
**analysis-only**: the server reads the bytes, answers, and stores nothing.
Low-confidence results are filed into `PROCESSED DOCUMENTS/_NEEDS_REVIEW/`
so a mis-detected name never hides among the good ones.

## Setup (macOS — one command)

```bash
cd local_tools
./install.sh
```

That installs the script to a stable spot, adds a `fp-file-it` terminal
command, walks you through login (password → macOS Keychain), and builds the
Finder **right-click → Quick Actions → Analyze & File** action for you. Re-run
it any time to update. You can then delete/move this repo — the installed copy
lives in `~/Library/Application Support/FPBudget/`.

### Or set up by hand

```bash
python3 fp_file_it.py --setup
# FPBudget URL [https://fp-budget.onrender.com]:  ⏎
# Login email:  you@example.com
# Password:     ••••••••   (stored in macOS Keychain, not on disk)
```

Uses your normal FPBudget login. Config lives in
`~/.config/fpbudget/analyzer.json`; the password goes in the macOS Keychain
(service `fpbudget.analyzer`). On non-Mac systems set `$FPBUDGET_PASSWORD`.

## Use from the terminal

```bash
python3 fp_file_it.py receipt.jpg invoice.pdf         # analyze + file
python3 fp_file_it.py --dry-run *.pdf                 # preview, move nothing
python3 fp_file_it.py --notify scan.heic              # + macOS notification
```

Accepts PDF, JPG, PNG, HEIC (HEIC comes back transcoded to JPEG, matching the
in-app analyzer). Files already inside `PROCESSED DOCUMENTS/` or
`SOURCE DOCUMENTS/` are skipped so you can't double-file.

## Use from Finder (right-click → Quick Action)

`./install.sh` already created this for you. Right-click any document(s) in
Finder → **Quick Actions → Analyze & File**. A notification reports
"3 filed, 1 to review" when it's done. The first run may prompt for Keychain
access — click **Always Allow**.

If the menu item doesn't show up immediately, open **System Settings →
Keyboard → Keyboard Shortcuts → Services → Files and Folders** and tick
**Analyze & File** (or log out and back in).

### Building it by hand instead

1. Open **Automator** → New Document → **Quick Action**.
2. Set "Workflow receives current" to **files or folders** in **Finder**.
3. Add a **Run Shell Script** action — Shell: `/bin/zsh`, Pass input:
   **as arguments**:

   ```zsh
   /usr/bin/python3 "$HOME/Library/Application Support/FPBudget/fp_file_it.py" --notify "$@"
   ```

4. Save as **"Analyze & File"**.

Tip: for a Dock droplet, choose **Application** instead of Quick Action with
the same shell action, save it to `/Applications`, and drag files onto its icon.

## Notes

- The first request after the Render service has been idle can take up to a
  minute (cold start); subsequent files run in ~5–20 s each.
- Works inside any synced folder too (Dropbox, iCloud Drive) — it's plain
  filesystem moves, the sync client picks them up like any other change.
- This tool never touches FPBudget projects. For project receipts use the
  in-app portal (`/analyzer`) so docs land in the project Dropbox and flow
  into Actuals.
