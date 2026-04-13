"""
One-time script to get a Dropbox offline refresh token.
Run this locally, paste the code, then add the printed values to Render env vars.

Usage:
    python get_dropbox_token.py
"""
import webbrowser

APP_KEY    = input("Paste your Dropbox App Key: ").strip()
APP_SECRET = input("Paste your Dropbox App Secret: ").strip()

auth_url = (
    f"https://www.dropbox.com/oauth2/authorize"
    f"?client_id={APP_KEY}"
    f"&response_type=code"
    f"&token_access_type=offline"
)

print(f"\nCopy and paste this URL into your browser:\n\n{auth_url}\n")
print("After authorizing, Dropbox will show you a code. Copy it.\n")

code = input("Paste the authorization code from Dropbox: ").strip()

import urllib.request, urllib.parse, base64, json

creds = base64.b64encode(f"{APP_KEY}:{APP_SECRET}".encode()).decode()
data  = urllib.parse.urlencode({
    "code":       code,
    "grant_type": "authorization_code",
}).encode()

req = urllib.request.Request(
    "https://api.dropbox.com/oauth2/token",
    data=data,
    headers={"Authorization": f"Basic {creds}",
             "Content-Type": "application/x-www-form-urlencoded"},
)

try:
    resp = json.loads(urllib.request.urlopen(req).read())
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"\n❌ Dropbox returned an error: {e.code}\n{body}")
    raise

print("\n✅ Add these to Render environment variables:\n")
print(f"  DROPBOX_APP_KEY      = {APP_KEY}")
print(f"  DROPBOX_APP_SECRET   = {APP_SECRET}")
print(f"  DROPBOX_REFRESH_TOKEN = {resp['refresh_token']}")
print("\nRefresh token does not expire. Keep it secret.")
