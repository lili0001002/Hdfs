# Windows SSH from Claude cloud sessions

The existing SessionStart entry runs `tailscale-up.sh`. It first invokes
`restore-windows-ssh.sh`, then continues the existing Tailscale startup. Both
scripts skip local sessions. The Windows target uses OpenSSH, not a Tailscale
SSH server.

Configure the personal cloud environment with:

- `TS_AUTHKEY`: the existing Tailscale auth key.
- `WIN_SSH_KEY`: the private key matching an authorized Windows public key,
  either raw OpenSSH/PEM text or Base64. It must be usable without an interactive
  passphrase prompt. Do not store the value in this repository or hook settings.

Cloud environment variables are readable by session processes and users of that
environment. Base64 is encoding, not encryption. Changes to these variables
apply to newly created sessions. Clearing the variable in the hook child process
does not remove it from the parent session's environment.

The restore hook validates the key in a temporary file before replacing the
dedicated `~/.ssh/claude_win_ed25519`. An absent variable leaves existing SSH
files unchanged. Invalid or encrypted keys fail without replacing existing
files. Changed existing files receive private gzip backups under `~/.ssh`.
The default `id_ed25519` is preserved.

SSH configuration is scoped to `100.102.200.115` and alias `win-tailnet`, with
user `Administrator`, key authentication, and strict host-key checking. The
pinned Ed25519 host public key was read directly from the Windows host on
2026-09-23. A future host-key change requires verification on that computer
before updating the pin.

`ProxyCommand` uses `/opt/tailscale/tailscale` and the same daemon socket as the
startup hook. This also handles containers with userspace networking and no TUN
device. Tailscale access rules must allow TCP 22 to the Windows target, and its
OpenSSH server must authorize the supplied key.

After starting a new cloud session with this commit and environment:

```sh
ssh -o BatchMode=yes Administrator@100.102.200.115 whoami
# Alternatively:
ssh win-tailnet whoami
```

Expected account: `wim-20230606dve\administrator`. A hook success message only
confirms file setup; this command verifies the actual network login.

Run the offline checks with `python3 tests/test_windows_ssh_hook.py`. They use
temporary test keys, never a real Windows credential, and never contact the
target. `WIN_SSH_DIR` is the test override for the SSH directory; leave it unset
in normal sessions. Windows Git Bash checks do not prove Linux permission
enforcement or cloud connectivity; validate those in the new cloud session.

References:

- https://code.claude.com/docs/en/cloud-environments#set-environment-variables
- https://tailscale.com/docs/reference/tailscale-cli
- https://tailscale.com/docs/concepts/userspace-networking
