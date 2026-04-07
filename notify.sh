#!/bin/bash
# FPBudget — send a push notification to Steven's phone via ntfy
# Usage: ./notify.sh "message here"
#        notify "message"  (if sourced or aliased)
TOPIC="fpbudget-2UNogKZFtFM"
MSG="${1:-FPBudget: task complete}"
curl -s -d "$MSG" "ntfy.sh/$TOPIC" > /dev/null
