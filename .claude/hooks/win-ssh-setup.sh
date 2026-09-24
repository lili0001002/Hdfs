#!/bin/bash
# SessionStart hook: prepare passwordless SSH to the Windows host over the tailnet.
# Requires WIN_SSH_KEY (ed25519 private key, raw OpenSSH PEM or base64 of it).
# Optional: WIN_SSH_KNOWN_HOSTS (pinned host key line(s); otherwise trust on first use),
# WIN_SSH_HOST (default 100.102.200.115), WIN_SSH_USER (default administrator).
# Result: `ssh win-tailnet "hostname & whoami"` works (remote shell is cmd.exe).
set -uo pipefail
[ "${CLAUDE_CODE_REMOTE:-}" = "true" ] || exit 0
[ -n "${WIN_SSH_KEY:-}" ] || { echo "win-ssh: WIN_SSH_KEY not set, skipping" >&2; exit 0; }

WIN_HOST="${WIN_SSH_HOST:-100.102.200.115}"; WIN_USER="${WIN_SSH_USER:-administrator}"
SSH_DIR="$HOME/.ssh"; KEY="$SSH_DIR/claude_win_ed25519"; KNOWN="$SSH_DIR/claude_win_known_hosts"
CONF="$SSH_DIR/claude_win_config"; MAIN_CONF="$SSH_DIR/config"; INCLUDE_LINE="Include \"$CONF\""

if ! command -v ssh >/dev/null || ! command -v ssh-keygen >/dev/null; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get install -y -qq openssh-client >/dev/null 2>&1 \
    || { apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq openssh-client >/dev/null 2>&1; } \
    || { echo "win-ssh: failed to install openssh-client" >&2; exit 0; }
fi

umask 077; mkdir -p "$SSH_DIR"; chmod 700 "$SSH_DIR"

# Private key (never echoed): raw PEM (real or literal "\n" newlines) or base64 of it.
tmp_key=$(mktemp "$SSH_DIR/.claude_win_key.XXXXXX")
if printf '%s' "$WIN_SSH_KEY" | grep -q 'BEGIN OPENSSH PRIVATE KEY'; then
  printf '%b\n' "$WIN_SSH_KEY" | sed 's/\r$//' >"$tmp_key"
else
  printf '%s' "$WIN_SSH_KEY" | tr -d ' \r\n' | base64 -d >"$tmp_key" 2>/dev/null
fi
ssh-keygen -l -f "$tmp_key" >/dev/null 2>&1 || { rm -f "$tmp_key"; echo "win-ssh: WIN_SSH_KEY invalid" >&2; exit 0; }
mv -f "$tmp_key" "$KEY"; chmod 600 "$KEY"

if [ -n "${WIN_SSH_KNOWN_HOSTS:-}" ]; then
  printf '%b\n' "$WIN_SSH_KNOWN_HOSTS" | sed 's/\r$//' >"$KNOWN"; strict=yes
else
  : >>"$KNOWN"; strict=accept-new
  echo "win-ssh: WIN_SSH_KNOWN_HOSTS not set, trusting host key on first use" >&2
fi
chmod 600 "$KNOWN"

cat >"$CONF" <<EOF
Host win-tailnet
    HostName $WIN_HOST
    User $WIN_USER
    IdentityFile $KEY
    IdentitiesOnly yes
    UserKnownHostsFile $KNOWN
    StrictHostKeyChecking $strict
    BatchMode yes
    ConnectTimeout 15
EOF
chmod 600 "$CONF"

touch "$MAIN_CONF"
if [ "$(head -n1 "$MAIN_CONF")" != "$INCLUDE_LINE" ]; then
  # grep exits 1 when nothing is left (e.g. empty config); that must not skip the mv.
  { echo "$INCLUDE_LINE"; grep -vxF "$INCLUDE_LINE" "$MAIN_CONF" || true; } >"$MAIN_CONF.tmp" && mv -f "$MAIN_CONF.tmp" "$MAIN_CONF"
fi
chmod 600 "$MAIN_CONF"
echo "win-ssh: key $(ssh-keygen -l -f "$KEY" | awk '{print $1, $2, $NF}')"

# Hooks run in parallel: wait for the tailnet before the self-test.
for _ in $(seq 60); do command -v tailscale >/dev/null && tailscale ip -4 >/dev/null 2>&1 && break; sleep 1; done
if out=$(ssh win-tailnet "hostname & whoami" 2>&1); then
  echo "win-ssh: connected -> $(echo "$out" | tr -d '\r' | paste -sd' ')"
else
  echo "win-ssh: self-test failed: $(echo "$out" | tail -1)" >&2
fi
exit 0
