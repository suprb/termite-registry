#!/usr/bin/env python3
"""Validate the registry: schema, files present, every shader compiles,
every theme parses. CI runs this on every PR; merged = published.

Usage: tools/validate.py   (from anywhere; paths resolve from the repo root)
Exit 0 = publishable.
"""
import hashlib
import json
import re
import stat
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parent.parent
KINDS = {"shader", "theme", "rig", "patch", "plugin", "channel"}
CHANNEL_MODES = {"two-way", "inbound-only", "read-only"}
CHANNEL_FIELD_TYPES = {
    "text", "secret", "boolean", "integer", "string-list", "integer-list",
    "path", "json",
}
CHANNEL_FIELD_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
REQUIRED = ["kind", "id", "name", "description", "author", "license", "version"]

errors = []


def err(msg):
    errors.append(msg)
    print(f"  ✗ {msg}")


def check_shader(entry, path: Path):
    if "termite_main" not in path.read_text():
        err(f"{entry['id']}: no termite_main")
        return
    preamble = (ROOT / "tools" / "preamble.metal").read_text()
    with tempfile.NamedTemporaryFile(suffix=".metal", mode="w", delete=False) as f:
        f.write(preamble + "\n" + path.read_text())
        tmp = f.name
    r = subprocess.run(
        ["xcrun", "-sdk", "macosx", "metal", "-c", tmp, "-o", "/dev/null"],
        capture_output=True, text=True)
    if r.returncode != 0:
        err(f"{entry['id']}: shader does not compile:\n{r.stderr[:400]}")


def check_theme(entry, path: Path):
    try:
        t = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        err(f"{entry['id']}: theme is not valid JSON ({e})")
        return
    for key in ("name", "background", "foreground", "ansi"):
        if key not in t:
            err(f"{entry['id']}: theme missing '{key}'")
    if len(t.get("ansi", [])) != 16:
        err(f"{entry['id']}: theme needs exactly 16 ansi colors")


def check_rig(entry, path: Path):
    known = {"theme", "shader", "font-family", "font-size", "line-height",
             "cursor-style", "cursor-blink", "hide-border", "margin",
             "opacity", "blur", "ghost-text", "smooth-cursor"}
    for n, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            err(f"{entry['id']}: rig line {n} is not key = value")
            continue
        key = line.split("=", 1)[0].strip()
        if key not in known:
            err(f"{entry['id']}: rig uses unknown key '{key}'")


def check_channel_archive(entry, path: Path):
    try:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            if len(names) > 128:
                err(f"{entry['id']}: Channel archive has too many files")
                return
            for name in names:
                member = PurePosixPath(name)
                if member.is_absolute() or ".." in member.parts:
                    err(f"{entry['id']}: archive contains unsafe path '{name}'")
                    return
                if member.name in {"config.json", ".env", ".env.local"}:
                    err(f"{entry['id']}: archive must not ship live configuration or secrets")
                    return
            manifests = [name for name in names
                         if len(PurePosixPath(name).parts) == 2
                         and PurePosixPath(name).name == "manifest.json"]
            if len(manifests) != 1:
                err(f"{entry['id']}: Channel archive needs exactly one root manifest.json")
                return
            manifest = json.loads(archive.read(manifests[0]))
            if not isinstance(manifest, dict):
                err(f"{entry['id']}: Channel manifest is not a JSON object")
                return
            if manifest.get("manifestVersion") != 1:
                err(f"{entry['id']}: Channel archive needs a v1 manifest")
            if manifest.get("id") != entry["id"]:
                err(f"{entry['id']}: archive manifest id does not match registry id")
            if manifest.get("version") != entry["version"]:
                err(f"{entry['id']}: archive manifest version does not match registry version")
            capabilities = manifest.get("capabilities")
            if not isinstance(capabilities, list) or "channels" not in capabilities:
                err(f"{entry['id']}: archive manifest does not request channels")
            declared = entry.get("capabilities")
            if isinstance(capabilities, list) and isinstance(declared, list):
                if set(capabilities) != set(declared):
                    err(f"{entry['id']}: archive and registry capabilities do not match")
            entrypoint = manifest.get("entrypoint")
            if not isinstance(entrypoint, str) or not entrypoint:
                err(f"{entry['id']}: archive manifest has no entrypoint")
                return
            executable = str(PurePosixPath(manifests[0]).parent / entrypoint)
            if executable not in names:
                err(f"{entry['id']}: archive entrypoint is missing")
                return
            mode = archive.getinfo(executable).external_attr >> 16
            if stat.S_ISLNK(mode):
                err(f"{entry['id']}: archive entrypoint cannot be a symlink")
            if not mode & 0o111:
                err(f"{entry['id']}: archive entrypoint is not executable")
    except (OSError, UnicodeDecodeError, zipfile.BadZipFile,
            json.JSONDecodeError) as exc:
        err(f"{entry['id']}: invalid Channel archive ({exc})")


def check_extension(entry):
    if not (entry.get("url") or entry.get("file")):
        err(f"{entry['id']}: {entry['kind']} needs 'url' or 'file'")
    if entry.get("url") and not entry.get("sha256"):
        err(f"{entry['id']}: remote {entry['kind']} needs 'sha256'")
    if entry.get("kind") == "channel":
        capabilities = entry.get("capabilities")
        if not isinstance(capabilities, list) or "channels" not in capabilities:
            err(f"{entry['id']}: Channel metadata must declare the channels capability")
        mode = entry.get("mode")
        if mode not in CHANNEL_MODES:
            err(f"{entry['id']}: Channel mode must be one of {sorted(CHANNEL_MODES)}")
        if not isinstance(entry.get("setup"), str) or not entry["setup"].strip():
            err(f"{entry['id']}: Channel metadata needs a concise setup description")
        if mode == "two-way" and isinstance(capabilities, list) and "events.read" not in capabilities:
            err(f"{entry['id']}: two-way Channel needs events.read for approved replies")
        if mode in {"read-only", "inbound-only"} and isinstance(capabilities, list) and "events.read" in capabilities:
            err(f"{entry['id']}: non-replying Channel must not request events.read")
        configuration = entry.get("configuration")
        if not isinstance(configuration, dict) or configuration.get("version") != 1:
            err(f"{entry['id']}: Channel needs a v1 configuration contract")
        else:
            fields = configuration.get("fields")
            if not isinstance(fields, list) or len(fields) > 32:
                err(f"{entry['id']}: configuration fields must be an array of at most 32 entries")
            else:
                keys = set()
                for index, field in enumerate(fields):
                    label = f"{entry['id']}: configuration field {index + 1}"
                    if not isinstance(field, dict):
                        err(f"{label} must be an object")
                        continue
                    key = field.get("key")
                    if not isinstance(key, str) or not CHANNEL_FIELD_KEY.fullmatch(key):
                        err(f"{label} has an invalid key")
                    elif key in keys:
                        err(f"{entry['id']}: duplicate configuration key '{key}'")
                    else:
                        keys.add(key)
                    if not isinstance(field.get("label"), str) or not field["label"].strip():
                        err(f"{label} needs a display label")
                    if field.get("type") not in CHANNEL_FIELD_TYPES:
                        err(f"{label} has an unknown type")
                    if not isinstance(field.get("required"), bool):
                        err(f"{label} needs a boolean required flag")
                    secret = field.get("type") == "secret"
                    service = field.get("keychainService")
                    if secret and (not isinstance(service, str) or not service.startswith("termite.")):
                        err(f"{label} secret needs a termite.* Keychain service")
                    if not secret and service is not None:
                        err(f"{label} cannot declare Keychain storage unless it is secret")
                    if secret and "default" in field:
                        err(f"{label} must not include a secret default")
                    for text_key in ("placeholder", "help"):
                        text = field.get(text_key)
                        if text is not None and (not isinstance(text, str) or len(text.encode()) > 512):
                            err(f"{label} {text_key} must be bounded text")

    rel = entry.get("file")
    if not rel:
        return
    path = (ROOT / rel).resolve()
    try:
        path.relative_to(ROOT.resolve())
    except ValueError:
        err(f"{entry['id']}: archive path leaves the registry")
        return
    if not path.is_file():
        err(f"{entry['id']}: archive missing: {rel}")
        return
    expected = entry.get("sha256")
    if not expected:
        err(f"{entry['id']}: local {entry['kind']} needs 'sha256'")
    else:
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            err(f"{entry['id']}: archive sha256 is {actual}, expected {expected}")
    if entry.get("kind") == "channel":
        check_channel_archive(entry, path)


def main():
    reg = json.loads((ROOT / "registry.json").read_text())
    entries = reg.get("entries", [])
    ids = set()
    print(f"validating {len(entries)} entries…")
    for e in entries:
        for key in REQUIRED:
            if key not in e:
                err(f"{e.get('id', '?')}: missing '{key}'")
        if e.get("kind") not in KINDS:
            err(f"{e.get('id', '?')}: unknown kind '{e.get('kind')}'")
        if e.get("id") in ids:
            err(f"{e.get('id')}: duplicate id")
        ids.add(e.get("id"))

        kind = e.get("kind")
        if kind in {"plugin", "channel"}:
            check_extension(e)
            continue

        rel = e.get("file")
        if not rel:
            err(f"{e.get('id', '?')}: content entry needs 'file'")
            continue
        path = ROOT / rel
        if not path.exists():
            err(f"{e['id']}: file missing: {rel}")
            continue
        if kind == "shader":
            check_shader(e, path)
        elif kind == "theme":
            check_theme(e, path)
        elif kind == "rig":
            check_rig(e, path)

    for f in reg.get("featured", []):
        if f.split(":", 1)[-1] not in ids:
            err(f"featured entry not in registry: {f}")

    source_channels = set()
    for manifest_path in (ROOT / "plugins").glob("*/manifest.json"):
        try:
            manifest = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            err(f"{manifest_path.relative_to(ROOT)}: invalid manifest ({exc})")
            continue
        if "channels" in manifest.get("capabilities", []):
            source_channels.add(manifest.get("id"))
    registry_channels = {
        entry.get("id") for entry in entries
        if entry.get("kind") == "channel"
        and str(entry.get("id", "")).startswith("dev.termite.")
    }
    if source_channels != registry_channels:
        for channel_id in sorted(source_channels - registry_channels):
            err(f"first-party Channel source is not registered: {channel_id}")
        for channel_id in sorted(registry_channels - source_channels):
            err(f"registered first-party Channel has no source: {channel_id}")

    if errors:
        print(f"\n{len(errors)} problem(s).")
        sys.exit(1)
    print("registry is publishable ✓")


if __name__ == "__main__":
    main()
