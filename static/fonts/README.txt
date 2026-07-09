Drop NotoEmoji-Regular.ttf here to enable real monochrome emoji in call-sheet PDFs.

Why: the Render Linux container has no emoji font, so weasyprint drops/garbles the
colour emojis on the travel line. By default the call sheet substitutes small Latin
text badges (FLT/HTL/CAR/VAN/RIDE/TEL) via the pdf_ico() macro in
templates/callsheet.html — these never depend on a font being installed.

To switch to real emoji instead of badges:
  1. Download NotoEmoji-Regular.ttf (monochrome, from the Google Noto Emoji repo)
     and place it in this directory as exactly:  NotoEmoji-Regular.ttf
  2. In templates/callsheet.html, in the pdf_ico() macro, remove the pdf_mode
     branch so the raw emoji is emitted in the PDF as well as on screen.
The pdf-only @font-face ('FP Emoji') that points at this file is already declared
in the pdf_mode <style> block of callsheet.html.
