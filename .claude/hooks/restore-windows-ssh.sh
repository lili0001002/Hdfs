#!/usr/bin/env bash
# Restore the user's authorized Windows SSH identity in Claude cloud sessions.
# Contains no private key. Invoke with bash; do not source this file.
set +x
set -euo pipefail

[[ "${CLAUDE_CODE_REMOTE:-}" == "true" ]] || exit 0
if [[ -z "${WIN_SSH_KEY:-}" ]]; then
  printf '%s\n' 'windows-ssh: WIN_SSH_KEY is absent; existing SSH files unchanged.' >&2
  exit 0
fi

umask 077
ssh_dir="${WIN_SSH_DIR:-$HOME/.ssh}"
target_host='100.102.200.115'
identity_file="$ssh_dir/claude_win_ed25519"
host_file="$ssh_dir/claude_win_known_hosts"
managed_config="$ssh_dir/claude_win_config"
main_config="$ssh_dir/config"
key_tmp=''
config_tmp=''
host_tmp=''
main_tmp=''
cleanup() {
  for temporary_file in "$key_tmp" "$config_tmp" "$host_tmp" "$main_tmp"; do
    [[ -z "$temporary_file" ]] || rm -f -- "$temporary_file"
  done
}
trap cleanup EXIT
fail() { printf 'windows-ssh: %s\n' "$1" >&2; exit 1; }

command -v ssh-keygen >/dev/null || fail 'ssh-keygen is unavailable.'
command -v base64 >/dev/null || fail 'base64 is unavailable.'
command -v gzip >/dev/null || fail 'gzip is unavailable.'
[[ "$ssh_dir" == /* ]] || fail 'SSH directory must be an absolute path.'
[[ "$ssh_dir" != *$'\n'* && "$ssh_dir" != *$'\r'* && "$ssh_dir" != *'"'* && "$ssh_dir" != *'%'* ]] || fail 'Unsupported SSH directory characters.'
[[ ! -L "$ssh_dir" ]] || fail 'SSH directory is a symbolic link; refusing to replace files.'
mkdir -p -- "$ssh_dir"
chmod 700 -- "$ssh_dir"
for destination in "$identity_file" "$host_file" "$managed_config" "$main_config"; do
  [[ ! -L "$destination" ]] || fail 'An SSH destination is a symbolic link; no files replaced.'
  [[ ! -e "$destination" || -f "$destination" ]] || fail 'An SSH destination is not a regular file.'
done

key_tmp="$(mktemp "$ssh_dir/.claude-win-key.XXXXXXXX")"
if [[ "$WIN_SSH_KEY" == *'-----BEGIN '* ]]; then
  printf '%s' "$WIN_SSH_KEY" | tr -d '\r' > "$key_tmp"
  [[ -z "$(tail -c 1 -- "$key_tmp")" ]] || printf '\n' >> "$key_tmp"
else
  if ! printf '%s' "$WIN_SSH_KEY" | tr -d '[:space:]' | base64 --decode > "$key_tmp" 2>/dev/null; then
    fail 'WIN_SSH_KEY is invalid base64; existing identity retained.'
  fi
fi
unset WIN_SSH_KEY
chmod 600 -- "$key_tmp"
if ! ssh-keygen -y -P '' -f "$key_tmp" >/dev/null 2>&1; then
  fail 'WIN_SSH_KEY is not a usable unencrypted private key; existing identity retained.'
fi

# Pin the host public key read directly from the target Windows computer.
host_tmp="$(mktemp "$ssh_dir/.claude-win-host.XXXXXXXX")"
printf '%s\n' "$target_host ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIB2G2thVvXR9yVA4LHoLlKbbVMtXXYpOaAYc6xtqMdy/" > "$host_tmp"
config_tmp="$(mktemp "$ssh_dir/.claude-win-config.XXXXXXXX")"
cat > "$config_tmp" <<EOF
Host $target_host win-tailnet
    HostName $target_host
    User Administrator
    # Use the same daemon/socket as tailscale-up.sh, including userspace mode.
    ProxyCommand /opt/tailscale/tailscale --socket=/var/run/tailscale/tailscaled.sock nc %h %p
    IdentityFile "$identity_file"
    IdentitiesOnly yes
    BatchMode yes
    PreferredAuthentications publickey
    PasswordAuthentication no
    StrictHostKeyChecking yes
    UserKnownHostsFile "$host_file"
    HostKeyAlgorithms ssh-ed25519
    ConnectTimeout 10
    ConnectionAttempts 1
Host *
EOF

# Keep existing config verbatim and place a single managed Include first.
# End the included host block with Host * so the old config keeps its scope.
include_line="Include \"$managed_config\""
main_tmp="$(mktemp "$ssh_dir/.claude-win-main.XXXXXXXX")"
if [[ -f "$main_config" ]] && [[ "$(head -n 1 -- "$main_config")" == "$include_line" ]]; then
  cat -- "$main_config" > "$main_tmp"
else
  printf '%s\n' "$include_line" > "$main_tmp"
  [[ ! -f "$main_config" ]] || cat -- "$main_config" >> "$main_tmp"
fi
chmod 600 -- "$host_tmp" "$config_tmp" "$main_tmp"

# Back up each changed existing file before replacing it. Backups stay private
# beside the SSH config, never in the repository or in hook output.
backup_file() {
  local existing="$1" replacement="$2" backup_path
  if [[ -f "$existing" ]] && ! cmp -s -- "$existing" "$replacement"; then
    backup_path="$(mktemp "$existing.before-claude-win.XXXXXXXX.gz")"
    gzip -c -- "$existing" > "$backup_path"
    chmod 600 -- "$backup_path"
  fi
}
backup_file "$identity_file" "$key_tmp"
backup_file "$host_file" "$host_tmp"
backup_file "$managed_config" "$config_tmp"
backup_file "$main_config" "$main_tmp"

mv -f -- "$key_tmp" "$identity_file"; key_tmp=''
mv -f -- "$host_tmp" "$host_file"; host_tmp=''
mv -f -- "$config_tmp" "$managed_config"; config_tmp=''
mv -f -- "$main_tmp" "$main_config"; main_tmp=''
printf '%s\n' 'windows-ssh: identity installed and Windows host key pinned; network login has not been tested.' >&2
