#!/bin/bash
# SessionStart hook: join the tailnet so services on the Tailscale host are reachable.
# Requires TS_AUTHKEY (reusable + ephemeral auth key) in the cloud environment's variables.
# Optional: TS_HOSTNAME, TS_EXTRA_ARGS (e.g. "--accept-routes"), TS_TARGET (host/IP to ping after connecting).
set -uo pipefail

# Only run in Claude Code on the web containers.
[ "${CLAUDE_CODE_REMOTE:-}" = "true" ] || exit 0

if [ -z "${TS_AUTHKEY:-}" ]; then
  echo "tailscale: TS_AUTHKEY not set, skipping tailnet connection" >&2
  exit 0
fi

TS_DIR=/opt/tailscale
SOCK=/var/run/tailscale/tailscaled.sock
TS="$TS_DIR/tailscale --socket=$SOCK"

if [ ! -x "$TS_DIR/tailscaled" ]; then
  version=$(curl -fsSL "https://pkgs.tailscale.com/stable/?mode=json" \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["TarballsVersion"])') || exit 0
  mkdir -p "$TS_DIR"
  curl -fsSL "https://pkgs.tailscale.com/stable/tailscale_${version}_amd64.tgz" \
    | tar xz --strip-components=1 -C "$TS_DIR" || { echo "tailscale: download failed" >&2; exit 0; }
fi

mkdir -p /var/lib/tailscale /var/run/tailscale
if ! pgrep -x tailscaled >/dev/null; then
  mode_args=()
  # Fall back to userspace networking (SOCKS5/HTTP proxy on :1055) if there is no TUN device.
  if [ ! -c /dev/net/tun ]; then
    mode_args=(--tun=userspace-networking --socks5-server=localhost:1055 --outbound-http-proxy-listen=localhost:1055)
  fi
  nohup "$TS_DIR/tailscaled" --state=mem: --socket="$SOCK" "${mode_args[@]}" \
    >/tmp/tailscaled.log 2>&1 &
  for _ in $(seq 20); do [ -S "$SOCK" ] && break; sleep 0.5; done
fi

# shellcheck disable=SC2086
if ! $TS up --authkey="$TS_AUTHKEY" \
    --hostname="${TS_HOSTNAME:-claude-code-web}" \
    --timeout=30s ${TS_EXTRA_ARGS:-}; then
  echo "tailscale: 'tailscale up' failed, see /tmp/tailscaled.log" >&2
  exit 0
fi

cat >/usr/local/bin/tailscale <<WRAP
#!/bin/sh
exec $TS_DIR/tailscale --socket=$SOCK "\$@"
WRAP
chmod +x /usr/local/bin/tailscale

echo "tailscale: connected as $($TS ip -4 2>/dev/null | head -1)"
if [ -n "${TS_TARGET:-}" ]; then
  $TS ping --c=1 --timeout=10s "$TS_TARGET" || echo "tailscale: $TS_TARGET not reachable yet" >&2
fi
exit 0
