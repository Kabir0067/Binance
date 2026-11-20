"""
Деплойи худкор ба Ubuntu VPS — танҳо як фармон:

    python runserver.py

Аз 0 то охир худкор:
  1. requirements.txt ва .gitignore-ро месозад/навсозӣ мекунад (.env ҳеҷ гоҳ ба git намеравад).
  2. Git: агар репо/remote бошад — commit + push (GIT_REPO_URL — барои пайваст кардани репои нав).
  3. SSH (paramiko): бори аввал бо парол ворид шуда, калиди SSH-и шахсиро насб мекунад —
     минбаъд бе парол. Калиди хост сабт ва тафтиш мешавад (ҳимоя аз MITM).
  4. Дар сервер:
       – python3 / venv / git (агар набошад — apt);
       – код: аз git (clone/pull), агар git дастнорас бошад — SFTP;
       – .env (SFTP, chmod 600);
       – venv (сохтан / таъмир) + pip install + санҷиши синтаксис;
       – systemd-service (сохтан / навсозӣ, enable, restart);
       – санҷиши саломатӣ ва логҳои охирин.

Парол ҳеҷ ҷо сабт намешавад: аз VPS_PASSWORD (муҳит) ё пурсиши бехатар (getpass).
"""

from __future__ import annotations

import getpass
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except (AttributeError, ValueError):
        pass

# ═══════════════════════════════ Сервер ════════════════════════════════
VPS_HOST = os.getenv("VPS_HOST", "185.184.71.29").strip()
VPS_USER = os.getenv("VPS_USER", "delus").strip()
VPS_PORT = int(os.getenv("VPS_PORT", "22").strip() or 22)
REMOTE_DIR = os.getenv("REMOTE_PROJECT_DIR", "Binance").strip()        # нисбат ба $HOME ё роҳи мутлақ
SERVICE_NAME = os.getenv("SYSTEMD_SERVICE_NAME", "binance-bot").strip() or "binance-bot"

GIT_REPO_URL = os.getenv("GIT_REPO_URL", "").strip()   # масалан https://github.com/<user>/<repo>.git (холӣ → SFTP)
GIT_REMOTE = "origin"
GIT_BRANCH = os.getenv("GIT_BRANCH", "main").strip() or "main"

SSH_KEY_PATH = Path.home() / ".ssh" / "binance_vps_ed25519"
KNOWN_HOSTS = Path.home() / ".ssh" / "known_hosts_binance_vps"

REQUIREMENTS = (
    "aiohttp>=3.9,<4.0",
    "pyTelegramBotAPI>=4.20,<5.0",
    "python-dotenv>=1.0,<2.0",
)
DEPLOY_GLOBS = ("Trade/*.py", "requirements.txt", ".gitignore", "runserver.py")
GITIGNORE = (
    ".env", ".env.*", ".venv/", "venv/", "__pycache__/", "*.py[cod]",
    "Logs/", "Data/", "*.log", ".idea/", ".vscode/",
)
HEALTH_WAIT_SEC = 10


class DeployError(RuntimeError):
    pass


def step(n: int, title: str) -> None:
    print(f"\n\033[1;36m[{n}/7] {title}\033[0m")


def ok(msg: str) -> None:
    print(f"   \033[32m✔\033[0m {msg}")


def warn(msg: str) -> None:
    print(f"   \033[33m⚠\033[0m {msg}")


# ═══════════════════════════════ Маҳаллӣ ═══════════════════════════════
def prepare_local_files() -> None:
    (PROJECT_ROOT / "requirements.txt").write_text("\n".join(REQUIREMENTS) + "\n", encoding="utf-8")
    ok("requirements.txt навсозӣ шуд")

    path = PROJECT_ROOT / ".gitignore"
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    missing = [x for x in GITIGNORE if x not in lines]
    if missing:
        path.write_text("\n".join([*lines, *missing]).strip() + "\n", encoding="utf-8")
    ok(".gitignore: .env, venv, Logs, Data ҳифз шудаанд")

    if not (PROJECT_ROOT / ".env").exists():
        raise DeployError(".env ёфт нашуд — BINANCE_API_KEY, BINANCE_API_SECRET, TELE_BOT_API, ADMIN_ID лозим")

    for py in sorted((PROJECT_ROOT / "Trade").glob("*.py")):
        result = subprocess.run([sys.executable, "-m", "py_compile", str(py)], capture_output=True, text=True)
        if result.returncode != 0:
            raise DeployError(f"Хатои синтаксис дар {py.name}:\n{result.stderr}")
    ok("Санҷиши синтаксис: ҳамаи модулҳо дурустанд")


def _git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(["git", *args], cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8")
    if check and result.returncode != 0:
        raise DeployError(f"git {' '.join(args)}: {result.stderr.strip() or result.stdout.strip()}")
    return result


def git_sync() -> str | None:
    """Commit + push. Бармегардонад URL-и репо (барои сервер) ё None (→ SFTP)."""
    if shutil.which("git") is None:
        warn("git дар компютер нест — код бо SFTP фиристода мешавад")
        return None
    if not (PROJECT_ROOT / ".git").exists():
        if not GIT_REPO_URL:
            warn("Git-репо нест ва GIT_REPO_URL холӣ — код бо SFTP фиристода мешавад")
            return None
        _git("init")
        _git("symbolic-ref", "HEAD", f"refs/heads/{GIT_BRANCH}")
        ok("Git-репо сохта шуд")

    url = _git("remote", "get-url", GIT_REMOTE, check=False).stdout.strip()
    if not url and GIT_REPO_URL:
        _git("remote", "add", GIT_REMOTE, GIT_REPO_URL)
        url = GIT_REPO_URL

    if _git("ls-files", "--error-unmatch", ".env", check=False).returncode == 0:
        _git("rm", "--cached", "-q", ".env")
        warn(".env аз git хориҷ шуд (махфӣ аст)")

    _git("add", "-A")
    if _git("diff", "--cached", "--quiet", check=False).returncode != 0:
        identity: list[str] = []
        if not _git("config", "user.email", check=False).stdout.strip():
            identity = ["-c", "user.name=deploy-bot", "-c", "user.email=deploy-bot@localhost"]
        stamp = time.strftime("%Y-%m-%d %H:%M")
        result = subprocess.run(["git", *identity, "commit", "-q", "-m", f"deploy: {stamp}"],
                                cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8")
        if result.returncode != 0:
            raise DeployError(f"git commit: {result.stderr.strip() or result.stdout.strip()}")
        ok(f"Commit: deploy {stamp}")
    else:
        ok("Тағйироти нав нест")

    if not url:
        warn("Git remote нест — код бо SFTP фиристода мешавад")
        return None
    push = _git("push", "-u", GIT_REMOTE, f"HEAD:{GIT_BRANCH}", check=False)
    if push.returncode != 0:
        warn(f"git push ноком: {push.stderr.strip()[:200]} — код бо SFTP фиристода мешавад")
        return None
    ok(f"Push → {GIT_REMOTE}/{GIT_BRANCH}")
    return url


# ═══════════════════════════════ SSH ═══════════════════════════════════
def _paramiko():
    try:
        import paramiko
    except ImportError:
        print("   paramiko насб мешавад…")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "paramiko>=3.4"], check=True)
        import paramiko
    return paramiko


@dataclass
class RemoteInfo:
    home: str
    uid: int
    python: str
    project_dir: str

    @property
    def is_root(self) -> bool:
        return self.uid == 0


class Remote:
    def __init__(self) -> None:
        self.pm = _paramiko()
        self.client = None
        self._password: str | None = os.getenv("VPS_PASSWORD") or None
        self._sudo_needs_password: bool | None = None
        self.info: RemoteInfo | None = None

    # ── пайвастшавӣ ──
    def password(self) -> str:
        if self._password is None:
            self._password = getpass.getpass(f"   🔑 Пароли {VPS_USER}@{VPS_HOST}: ")
        return self._password

    def _new_client(self):
        client = self.pm.SSHClient()
        if KNOWN_HOSTS.exists():
            client.load_host_keys(str(KNOWN_HOSTS))
        client.set_missing_host_key_policy(self.pm.AutoAddPolicy())
        return client

    def connect(self) -> None:
        pm = self.pm
        base = dict(hostname=VPS_HOST, port=VPS_PORT, username=VPS_USER, timeout=20, banner_timeout=30, auth_timeout=30)
        attempts = []
        if SSH_KEY_PATH.exists():
            attempts.append(("калиди деплой", dict(key_filename=str(SSH_KEY_PATH), look_for_keys=False, allow_agent=False)))
        attempts.append(("калидҳои SSH-и система", dict(look_for_keys=True, allow_agent=True)))

        for label, extra in attempts:
            client = self._new_client()
            try:
                client.connect(**base, **extra)
                self.client = client
                ok(f"Пайваст шуд ({label})")
                break
            except pm.BadHostKeyException as exc:
                raise DeployError(f"Калиди хост ИВАЗ шудааст ({exc}). Агар сервер нав нашуда бошад — ин MITM аст! "
                                  f"Агар нав шуда бошад: {KNOWN_HOSTS} -ро нест кунед.") from exc
            except (pm.AuthenticationException, pm.SSHException):
                client.close()
            except OSError as exc:
                raise DeployError(f"Сервер {VPS_HOST}:{VPS_PORT} дастнорас: {exc}") from exc

        if self.client is None:
            client = self._new_client()
            try:
                client.connect(**base, password=self.password(), look_for_keys=False, allow_agent=False)
            except pm.AuthenticationException as exc:
                raise DeployError("Парол нодуруст аст") from exc
            self.client = client
            ok("Пайваст шуд (парол)")
            self._install_key()

        KNOWN_HOSTS.parent.mkdir(parents=True, exist_ok=True)
        self.client.save_host_keys(str(KNOWN_HOSTS))

    def _install_key(self) -> None:
        pub_path = SSH_KEY_PATH.with_suffix(".pub")
        if not SSH_KEY_PATH.exists():
            SSH_KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
            keygen = shutil.which("ssh-keygen")
            if keygen:
                subprocess.run([keygen, "-q", "-t", "ed25519", "-N", "", "-C", "binance-deploy", "-f", str(SSH_KEY_PATH)],
                               check=True)
            else:
                key = self.pm.RSAKey.generate(3072)
                key.write_private_key_file(str(SSH_KEY_PATH))
                pub_path.write_text(f"ssh-rsa {key.get_base64()} binance-deploy\n", encoding="utf-8")
        pub = pub_path.read_text(encoding="utf-8").strip()
        q = shlex.quote(pub)
        self.run(
            "umask 077 && mkdir -p ~/.ssh && touch ~/.ssh/authorized_keys && "
            f"(grep -qxF {q} ~/.ssh/authorized_keys || echo {q} >> ~/.ssh/authorized_keys)",
            show=False,
        )
        ok("Калиди SSH насб шуд — деплойҳои навбатӣ бе парол")

    def close(self) -> None:
        if self.client is not None:
            self.client.close()

    # ── иҷрои фармон ──
    def run(self, command: str, *, sudo: bool = False, check: bool = True, show: bool = True) -> tuple[int, str]:
        assert self.client is not None
        feed = None
        if sudo and not (self.info and self.info.is_root):
            if self._sudo_needs_password is None:
                code, _ = self.run("sudo -n true", check=False, show=False)
                self._sudo_needs_password = code != 0
            prefix = "sudo -S -p '' " if self._sudo_needs_password else "sudo -n "
            command = f"{prefix}bash -c {shlex.quote(command)}"
            if self._sudo_needs_password:
                feed = self.password() + "\n"
        else:
            command = f"bash -c {shlex.quote(command)}"

        chan = self.client.get_transport().open_session()
        chan.set_combine_stderr(True)
        chan.exec_command(command)
        if feed:
            chan.sendall(feed.encode())
        chan.shutdown_write()

        out: list[str] = []
        buf = b""
        while True:
            data = chan.recv(4096)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                text = line.decode("utf-8", errors="replace")
                out.append(text)
                if show:
                    print(f"   │ {text}")
        if buf:
            out.append(buf.decode("utf-8", errors="replace"))
            if show:
                print(f"   │ {out[-1]}")
        code = chan.recv_exit_status()
        if check and code != 0:
            tail = "\n".join(out[-15:])
            raise DeployError(f"Фармон дар сервер ноком шуд (код {code}):\n{tail}")
        return code, "\n".join(out)

    def upload(self, local: Path, remote: str, mode: int | None = None) -> None:
        assert self.client is not None
        sftp = self.client.open_sftp()
        try:
            sftp.put(str(local), remote)
            if mode is not None:
                sftp.chmod(remote, mode)
        finally:
            sftp.close()

    def write_text(self, remote: str, content: str, mode: int = 0o644) -> None:
        assert self.client is not None
        sftp = self.client.open_sftp()
        try:
            with sftp.open(remote, "w") as fh:
                fh.write(content.encode("utf-8"))
            sftp.chmod(remote, mode)
        finally:
            sftp.close()


# ═══════════════════════════════ Сервер: қадамҳо ═══════════════════════
def inspect_server(r: Remote) -> RemoteInfo:
    _, out = r.run(
        'echo "HOME=$HOME"; echo "UID=$(id -u)"; '
        "echo \"PY=$(python3 -c 'import sys;print(\"%d.%d\" % sys.version_info[:2])' 2>/dev/null || echo none)\"; "
        'echo "GIT=$(command -v git || echo none)"; '
        "echo \"VENV=$(python3 -c 'import ensurepip, venv' >/dev/null 2>&1 && echo yes || echo no)\"",
        show=False,
    )
    values = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
    home = values.get("HOME", "").strip()
    project_dir = REMOTE_DIR if REMOTE_DIR.startswith("/") else f"{home}/{REMOTE_DIR}"
    info = RemoteInfo(home=home, uid=int(values.get("UID", "1000")), python=values.get("PY", "none").strip(),
                      project_dir=project_dir)
    r.info = info

    packages = []
    if info.python == "none":
        packages += ["python3", "python3-venv", "python3-pip"]
    elif values.get("VENV", "no").strip() != "yes":
        packages += ["python3-venv", "python3-pip"]
    if values.get("GIT", "none").strip() == "none":
        packages.append("git")
    if packages:
        print(f"   Насби пакетҳо: {' '.join(packages)}")
        r.run("export DEBIAN_FRONTEND=noninteractive; apt-get update -qq && apt-get install -y -qq " + " ".join(packages),
              sudo=True)
        _, py = r.run("python3 -c 'import sys;print(\"%d.%d\" % sys.version_info[:2])'", show=False)
        info.python = py.strip()

    major, minor = (int(x) for x in info.python.split(".")[:2])
    if (major, minor) < (3, 10):
        raise DeployError(f"Дар сервер Python {info.python} аст — ҳадди ақал 3.10 лозим (Ubuntu 22.04+)")
    ok(f"Сервер: Python {info.python}, корбар {'root' if info.is_root else VPS_USER}, папка {info.project_dir}")
    return info


def deploy_code(r: Remote, info: RemoteInfo, repo_url: str | None) -> None:
    d = shlex.quote(info.project_dir)
    if repo_url:
        code, _ = r.run(
            f"set -e; mkdir -p {d}; cd {d}; "
            "[ -d .git ] || { git init -q; git symbolic-ref HEAD refs/heads/" + shlex.quote(GIT_BRANCH) + "; }; "
            f"git remote remove {GIT_REMOTE} 2>/dev/null || true; "
            f"git remote add {GIT_REMOTE} {shlex.quote(repo_url)}; "
            f"GIT_TERMINAL_PROMPT=0 git fetch -q --depth 1 {GIT_REMOTE} {shlex.quote(GIT_BRANCH)}; "
            "git reset -q --hard FETCH_HEAD; git log -1 --format='%h %s'",
            check=False,
        )
        if code == 0:
            ok("Код аз git навсозӣ шуд")
            return
        warn("git дар сервер ноком шуд (репои хусусӣ?) — SFTP истифода мешавад")

    files = sorted({p for pattern in DEPLOY_GLOBS for p in PROJECT_ROOT.glob(pattern) if p.is_file()})
    dirs = sorted({f"{info.project_dir}/{p.relative_to(PROJECT_ROOT).parent.as_posix()}".rstrip("/.") for p in files})
    r.run("mkdir -p " + " ".join(shlex.quote(x) for x in dirs), show=False)
    for p in files:
        rel = p.relative_to(PROJECT_ROOT).as_posix()
        r.upload(p, f"{info.project_dir}/{rel}")
    ok(f"Код бо SFTP фиристода шуд ({len(files)} файл)")


def upload_env(r: Remote, info: RemoteInfo) -> None:
    r.upload(PROJECT_ROOT / ".env", f"{info.project_dir}/.env", mode=0o600)
    ok(".env фиристода шуд (chmod 600)")


def setup_venv(r: Remote, info: RemoteInfo) -> None:
    d = shlex.quote(info.project_dir)
    r.run(
        f"set -e; cd {d}; "
        "if [ ! -x .venv/bin/python ] || ! .venv/bin/python -c 'import sys' >/dev/null 2>&1; then "
        "  rm -rf .venv; python3 -m venv .venv; echo 'venv сохта шуд'; fi; "
        ".venv/bin/python -m pip install -q --disable-pip-version-check --upgrade pip wheel; "
        ".venv/bin/python -m pip install -q --disable-pip-version-check -r requirements.txt; "
        ".venv/bin/python -c 'import aiohttp, telebot, dotenv' && "
        ".venv/bin/python -m py_compile Trade/*.py && echo 'вобастагиҳо ва синтаксис: OK'; "
        "mkdir -p Logs Data",
    )
    ok("venv омода")


def _unit(info: RemoteInfo) -> str:
    d = info.project_dir
    system = info.is_root
    lines = [
        "[Unit]",
        "Description=Binance Spot Trading Bot",
        *(["After=network-online.target", "Wants=network-online.target"] if system else []),
        "StartLimitIntervalSec=0",
        "",
        "[Service]",
        "Type=simple",
        *(["User=root"] if system else []),
        f"WorkingDirectory={d}",
        f"ExecStart={d}/.venv/bin/python -u {d}/Trade/engine.py",
        "Restart=always",
        "RestartSec=5",
        "KillSignal=SIGTERM",
        "TimeoutStopSec=30",
        "Environment=PYTHONUNBUFFERED=1",
        "Environment=PYTHONDONTWRITEBYTECODE=1",
        "",
        "[Install]",
        f"WantedBy={'multi-user.target' if system else 'default.target'}",
        "",
    ]
    return "\n".join(lines)


def install_service(r: Remote, info: RemoteInfo) -> tuple[str, str]:
    svc = shlex.quote(SERVICE_NAME)
    if info.is_root:
        unit_path = f"/etc/systemd/system/{SERVICE_NAME}.service"
        sctl, jctl, env = "systemctl", f"journalctl -u {svc}", ""
    else:
        _, linger = r.run("loginctl show-user \"$USER\" -p Linger --value 2>/dev/null || true", show=False)
        if linger.strip() != "yes":
            code, _ = r.run("loginctl enable-linger \"$USER\"", check=False, show=False)
            if code != 0:
                r.run(f"loginctl enable-linger {shlex.quote(VPS_USER)}", sudo=True, show=False)
            ok("Linger фаъол шуд — бот бе login кор мекунад ва баъди reboot худ оғоз мешавад")
        env = ('export XDG_RUNTIME_DIR="/run/user/$(id -u)"; '
               'export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"; '
               'for i in $(seq 1 30); do [ -S "$XDG_RUNTIME_DIR/bus" ] && break; sleep 0.5; done; ')
        r.run("mkdir -p ~/.config/systemd/user", show=False)
        unit_path = f"{info.home}/.config/systemd/user/{SERVICE_NAME}.service"
        sctl, jctl = "systemctl --user", f"journalctl --user -u {svc}"

    r.write_text(unit_path, _unit(info))
    r.run(f"{env}{sctl} daemon-reload && {sctl} enable -q {svc} && {sctl} restart {svc}")
    ok(f"Service '{SERVICE_NAME}' насб ва аз нав оғоз шуд")
    return env + sctl, env + jctl


def health_check(r: Remote, info: RemoteInfo, sctl: str, jctl: str) -> None:
    svc = shlex.quote(SERVICE_NAME)
    print(f"   Интизорӣ {HEALTH_WAIT_SEC} сония…")
    time.sleep(HEALTH_WAIT_SEC)
    _, state = r.run(f"{sctl} is-active {svc} || true", show=False)
    _, restarts = r.run(f"{sctl} show {svc} -p NRestarts --value || echo 0", show=False)
    print("   ── логҳои охирин ──")
    code, _ = r.run(f"{jctl} -n 25 --no-pager -o cat 2>/dev/null | grep . ", check=False)
    if code != 0:
        r.run(f"tail -n 25 {shlex.quote(info.project_dir)}/Logs/*.log 2>/dev/null || true", check=False)
    n_restarts = int((restarts.strip().splitlines() or ["0"])[-1] or 0)
    if state.strip().splitlines()[-1:] != ["active"] or n_restarts > 0:
        raise DeployError(f"Service кор намекунад (ҳолат: {state.strip()}, рестартҳо: {n_restarts}). Логҳои болоро бинед.")
    ok("Бот дар сервер фаъол аст ✅")


# ═══════════════════════════════ main ═════════════════════════════════
def main() -> None:
    started = time.time()
    print(f"\033[1m🚀 Деплой → {VPS_USER}@{VPS_HOST}:{VPS_PORT}  service={SERVICE_NAME}\033[0m")
    remote: Remote | None = None
    try:
        step(1, "Омодасозии файлҳо")
        prepare_local_files()

        step(2, "Git")
        repo_url = git_sync()

        step(3, "Пайвастшавӣ ба сервер")
        remote = Remote()
        remote.connect()
        info = inspect_server(remote)

        step(4, "Код ва .env")
        deploy_code(remote, info, repo_url)
        upload_env(remote, info)

        step(5, "Муҳити Python (venv)")
        setup_venv(remote, info)

        step(6, "Service (systemd)")
        sctl, jctl = install_service(remote, info)

        step(7, "Санҷиши саломатӣ")
        health_check(remote, info, sctl, jctl)

        sctl_hint = "systemctl" if info.is_root else "systemctl --user"
        jctl_hint = "journalctl" if info.is_root else "journalctl --user"
        print(f"\n\033[1;32m✅ Тайёр! ({time.time() - started:.0f} сония)\033[0m")
        print(f"   Логҳо:   ssh {VPS_USER}@{VPS_HOST}  →  {jctl_hint} -u {SERVICE_NAME} -f")
        print(f"   Идора:   {sctl_hint} status|restart|stop {SERVICE_NAME}")
    except DeployError as exc:
        print(f"\n\033[1;31m❌ {exc}\033[0m", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹ Бекор шуд", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"\n\033[1;31m❌ Хатои ғайричашмдошт: {exc!r}\033[0m", file=sys.stderr)
        sys.exit(1)
    finally:
        if remote is not None:
            remote.close()


if __name__ == "__main__":
    main()
