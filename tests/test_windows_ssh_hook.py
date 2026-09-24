"""Offline regression checks; creates disposable keys, never contacts Windows."""
import base64
import gzip
import hashlib
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[1]
HOOK = REPO / ".claude/hooks/restore-windows-ssh.sh"
STARTUP = REPO / ".claude/hooks/tailscale-up.sh"
GIT_ROOT = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Git"
BASH = str(GIT_ROOT / "bin/bash.exe") if os.name == "nt" else shutil.which("bash")
KEYGEN = str(GIT_ROOT / "usr/bin/ssh-keygen.exe") if os.name == "nt" else shutil.which("ssh-keygen")
SSH = str(GIT_ROOT / "usr/bin/ssh.exe") if os.name == "nt" else shutil.which("ssh")


def shell_path(path):
    value = str(path.resolve()).replace("\\", "/")
    return "/" + value[0].lower() + value[2:] if os.name == "nt" else value


class WindowsSSHHookTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory(prefix="test-windows-ssh-")
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.key = self.root / "test-key"
        self.call([KEYGEN, "-q", "-t", "ed25519", "-N", "", "-f", str(self.key)])
        self.raw = self.key.read_text()
        self.encoded = base64.b64encode(self.raw.encode()).decode()
        self.ssh_dir = self.root / "ssh dir"
        self.env = dict(os.environ)
        self.env.update(CLAUDE_CODE_REMOTE="true", WIN_SSH_DIR=shell_path(self.ssh_dir))
        self.env.pop("WIN_SSH_KEY", None)
        self.env.pop("TS_AUTHKEY", None)

    def call(self, args, env=None, status=0):
        result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, status, "Unexpected exit code; output suppressed to protect keys")
        return result

    def restore(self, value=None, status=0, script=HOOK):
        env = dict(self.env)
        if value is not None:
            env["WIN_SSH_KEY"] = value
        return self.call([BASH, shell_path(script)], env=env, status=status)

    def snapshot(self):
        return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.ssh_dir.iterdir() if p.is_file()}

    def test_missing_variable_and_local_session_are_noops(self):
        self.restore()
        self.assertFalse(self.ssh_dir.exists())
        self.env["CLAUDE_CODE_REMOTE"] = "false"
        self.restore(self.encoded, script=STARTUP)
        self.assertFalse(self.ssh_dir.exists())

    def test_base64_preserves_default_key_and_existing_config(self):
        self.ssh_dir.mkdir()
        original = b"Host example.invalid\n    User preserved-user\n"
        (self.ssh_dir / "config").write_bytes(original)
        (self.ssh_dir / "id_ed25519").write_text("existing-default-key")
        self.restore(self.encoded)
        self.assertEqual((self.ssh_dir / "claude_win_ed25519").read_text(), self.raw)
        self.assertEqual((self.ssh_dir / "id_ed25519").read_text(), "existing-default-key")
        self.assertTrue((self.ssh_dir / "config").read_bytes().endswith(original))
        backup = next(self.ssh_dir.glob("config.before-claude-win.*.gz"))
        self.assertEqual(gzip.decompress(backup.read_bytes()), original)
        other = self.call([SSH, "-G", "-F", str(self.ssh_dir / "config"), "example.invalid"]).stdout
        self.assertIn("user preserved-user\n", other)
        self.assertNotIn("claude_win_ed25519", other)
        self.assertNotIn("proxycommand /opt/tailscale/", other)

    def test_repeat_and_raw_crlf_are_idempotent(self):
        self.restore(self.encoded)
        before = self.snapshot()
        self.restore(self.encoded)
        self.assertEqual(self.snapshot(), before)
        self.restore(self.raw.replace("\n", "\r\n"))
        self.assertEqual(self.snapshot(), before)
        self.restore(self.raw.rstrip("\n"))
        self.assertEqual(self.snapshot(), before)

    def test_invalid_or_encrypted_key_preserves_all_files(self):
        self.restore(self.encoded)
        before = self.snapshot()
        encrypted = self.root / "encrypted-key"
        self.call([KEYGEN, "-q", "-t", "ed25519", "-N", "test-only-passphrase", "-f", str(encrypted)])
        invalid_values = ["!!!invalid base64!!!", base64.b64encode(b"not a private key").decode(),
                          base64.b64encode(encrypted.read_bytes()).decode()]
        for value in invalid_values:
            with self.subTest(kind="invalid credential"):
                self.restore(value, status=1)
                self.assertEqual(self.snapshot(), before)

    def test_target_has_pinned_host_and_userspace_transport(self):
        self.restore(self.encoded)
        for target in ("100.102.200.115", "win-tailnet"):
            effective = self.call([SSH, "-G", "-F", str(self.ssh_dir / "config"), target]).stdout
            for expected in ("hostname 100.102.200.115\n", "user Administrator\n", "batchmode yes\n",
                             "identitiesonly yes\n", "stricthostkeychecking true\n", "claude_win_ed25519",
                             "claude_win_known_hosts", "proxycommand /opt/tailscale/tailscale",
                             "--socket=/var/run/tailscale/tailscaled.sock nc %h %p"):
                self.assertIn(expected, effective)
        known_hosts = (self.ssh_dir / "claude_win_known_hosts").read_text()
        self.assertTrue(known_hosts.startswith("100.102.200.115 ssh-ed25519 "))
        self.assertEqual(len(known_hosts.splitlines()), 1)

    def test_caller_trace_does_not_disclose_key(self):
        env = dict(self.env, WIN_SSH_KEY=self.encoded)
        result = self.call([BASH, "-x", shell_path(STARTUP)], env=env)
        output = result.stdout + result.stderr
        for secret in (self.raw.strip(), self.encoded, self.raw.splitlines()[1]):
            self.assertNotIn(secret, output)

    def test_startup_restores_before_missing_tailscale_auth_exit(self):
        result = self.restore(self.encoded, script=STARTUP)
        self.assertEqual((self.ssh_dir / "claude_win_ed25519").read_text(), self.raw)
        self.assertIn("TS_AUTHKEY not set", result.stderr)
        before = self.snapshot()
        result = self.restore("invalid!", status=1, script=STARTUP)
        self.assertEqual(self.snapshot(), before)
        self.assertIn("continuing Tailscale startup", result.stderr)
        self.assertIn("TS_AUTHKEY not set", result.stderr)

    @unittest.skipIf(os.name == "nt", "POSIX permission enforcement requires Linux/macOS")
    def test_posix_permissions(self):
        self.restore(self.encoded)
        self.assertEqual(stat.S_IMODE(self.ssh_dir.stat().st_mode), 0o700)
        for name in ("claude_win_ed25519", "claude_win_known_hosts", "claude_win_config", "config"):
            self.assertEqual(stat.S_IMODE((self.ssh_dir / name).stat().st_mode), 0o600)


if __name__ == "__main__":
    unittest.main(verbosity=2)
