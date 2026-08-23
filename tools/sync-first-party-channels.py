#!/usr/bin/env python3
"""Sync packaged first-party Channel metadata into registry.json."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / "registry.json"

# Marketplace-facing facts that are not executable manifest fields.
CHANNEL_METADATA = {
    "apple-reminders": ("read-only", "macOS Reminders list allowlist"),
    "clipboard-inbox": ("two-way", "Explicit macOS clipboard opt-in"),
    "command-queue": ("two-way", "Producer and consumer argv arrays"),
    "demo-inbox": ("two-way", "No account or credentials"),
    "discord": ("two-way", "Bot token and channel allowlist"),
    "folder-drop": ("two-way", "Explicit inbox and outbox folders"),
    "git-watch": ("read-only", "One local Git repository"),
    "github-issues": ("two-way", "Fine-grained token and repository allowlist"),
    "imessage": ("two-way", "Full Disk Access and chat allowlist"),
    "imap-mail": ("two-way", "IMAP account; SMTP optional"),
    "jira": ("two-way", "Jira URL, API token, and project allowlist"),
    "linear": ("two-way", "API token and team allowlist"),
    "mastodon": ("two-way", "Instance URL and access token"),
    "matrix": ("two-way", "Homeserver token and room allowlist"),
    "ntfy": ("two-way", "Server, topic allowlist, and optional token"),
    "rss-feed": ("read-only", "One or more HTTPS feed URLs"),
    "slack": ("two-way", "Bot token and channel ID"),
    "telegram": ("two-way", "Bot token and chat allowlist"),
    "webhook-inbox": ("two-way", "Shared secret; callback optional"),
}


def field(key: str, label: str, kind: str = "text", *, required: bool = False,
          default=None, placeholder: str = "", help_text: str = "",
          keychain_service: str | None = None) -> dict:
    """A small declarative setup contract consumed by Termite's native wizard."""
    value = {"key": key, "label": label, "type": kind, "required": required}
    if default is not None:
        value["default"] = default
    if placeholder:
        value["placeholder"] = placeholder
    if help_text:
        value["help"] = help_text
    if keychain_service:
        value["keychainService"] = keychain_service
    return value


# Keep this deliberately smaller than config.example.json: it is the minimum
# successful first-run setup, plus the explicit switches that permit local
# reads or outbound delivery. Advanced tuning remains editable in config.json.
CHANNEL_CONFIGURATION = {
    "apple-reminders": [
        field("enabled", "Enable Reminders access", "boolean", required=True, default=True),
        field("allowedListNames", "Allowed list names", "string-list", required=True,
              placeholder="Work, Termite", help_text="Only these Reminders lists are read."),
    ],
    "clipboard-inbox": [
        field("enabled", "Enable connector", "boolean", required=True, default=True),
        field("pollClipboard", "Read clipboard changes", "boolean", required=True, default=True),
        field("writeApprovedReplies", "Write approved replies", "boolean", default=False,
              help_text="Off by default; enabling replaces the current clipboard on delivery."),
    ],
    "command-queue": [
        field("enabled", "Enable command queue", "boolean", required=True, default=True),
        field("producer", "Producer argv", "json", required=True,
              placeholder='["/absolute/path/to/producer"]'),
        field("consumer", "Consumer argv", "json", required=True,
              placeholder='["/absolute/path/to/consumer"]'),
    ],
    "demo-inbox": [],
    "discord": [
        field("botToken", "Bot token", "secret", required=True,
              keychain_service="termite.discord"),
        field("channelIds", "Allowed channel IDs", "string-list", required=True,
              placeholder="123456789012345678"),
        field("account", "Account label", placeholder="workspace-bot"),
    ],
    "folder-drop": [
        field("enabled", "Enable folder access", "boolean", required=True, default=True),
        field("inbox", "Inbox folder", "path", required=True, placeholder="/path/to/inbox"),
        field("outbox", "Outbox folder", "path", required=True, placeholder="/path/to/outbox"),
    ],
    "git-watch": [
        field("enabled", "Enable repository watch", "boolean", required=True, default=True),
        field("repository", "Repository", "path", required=True, placeholder="/path/to/repository"),
        field("includeExistingCommits", "Import existing commits", "boolean", default=False),
    ],
    "github-issues": [
        field("token", "Fine-grained token", "secret", required=True,
              keychain_service="termite.github-issues"),
        field("repositories", "Allowed repositories", "string-list", required=True,
              placeholder="owner/repository"),
        field("account", "GitHub login", placeholder="octocat"),
    ],
    "imap-mail": [
        field("imap_host", "IMAP host", required=True, placeholder="imap.example.com"),
        field("username", "IMAP username", required=True, placeholder="you@example.com"),
        field("password", "IMAP password", "secret", required=True,
              keychain_service="termite.imap-mail"),
        field("mailbox", "Mailbox", default="INBOX"),
        field("smtp_host", "SMTP host", placeholder="smtp.example.com",
              help_text="Optional; required only for approved replies."),
        field("smtp_username", "SMTP username", placeholder="you@example.com"),
        field("smtp_password", "SMTP password", "secret",
              keychain_service="termite.imap-mail.smtp"),
        field("from_address", "From address", placeholder="you@example.com"),
    ],
    "imessage": [
        field("enabled", "Enable Messages access", "boolean", required=True, default=True),
        field("allowedHandles", "Allowed handles", "string-list", required=True,
              placeholder="+46700000000, person@example.com"),
        field("sendApprovedReplies", "Allow approved replies", "boolean", default=False),
    ],
    "jira": [
        field("baseUrl", "Jira site", required=True,
              placeholder="https://your-site.atlassian.net"),
        field("email", "Account email", required=True, placeholder="you@example.com"),
        field("apiToken", "API token", "secret", required=True,
              keychain_service="termite.jira"),
        field("projectKeys", "Allowed project keys", "string-list", required=True,
              placeholder="ENG, OPS"),
    ],
    "linear": [
        field("apiKey", "API key", "secret", required=True,
              keychain_service="termite.linear"),
        field("teamIds", "Allowed team IDs", "string-list", required=True,
              placeholder="00000000-0000-0000-0000-000000000000"),
        field("projectIds", "Allowed project IDs", "string-list"),
    ],
    "mastodon": [
        field("base_url", "Instance URL", required=True,
              placeholder="https://mastodon.social"),
        field("access_token", "Access token", "secret", required=True,
              keychain_service="termite.mastodon"),
        field("account", "Account label", required=True, placeholder="@you@example.social"),
    ],
    "matrix": [
        field("homeserver", "Homeserver", required=True,
              placeholder="https://matrix.example.com"),
        field("access_token", "Access token", "secret", required=True,
              keychain_service="termite.matrix"),
        field("room_ids", "Allowed room IDs", "string-list", required=True,
              placeholder="!room-id:example.com"),
        field("own_user_id", "Your Matrix user ID", required=True,
              placeholder="@you:example.com"),
    ],
    "ntfy": [
        field("server_url", "Server URL", required=True, default="https://ntfy.sh"),
        field("topics", "Subscribed topics", "string-list", required=True,
              placeholder="your-private-inbox"),
        field("access_token", "Access token", "secret", keychain_service="termite.ntfy"),
        field("reply_topic", "Reply topic", placeholder="termite-replies"),
    ],
    "rss-feed": [
        field("feed_urls", "Feed URLs", "string-list", required=True,
              placeholder="https://example.com/feed.xml"),
        field("bearer_token", "Bearer token", "secret",
              keychain_service="termite.rss-feed"),
    ],
    "slack": [
        field("botToken", "Bot token", "secret", required=True,
              keychain_service="termite.slack"),
        field("channelId", "Channel ID", required=True, placeholder="C0123456789"),
        field("account", "Workspace label", placeholder="workspace-name"),
    ],
    "telegram": [
        field("botToken", "Bot token", "secret", required=True,
              keychain_service="termite.telegram"),
        field("allowedChatIds", "Allowed chat IDs", "integer-list", required=True,
              placeholder="-1001234567890"),
        field("account", "Bot label", placeholder="my_bot"),
    ],
    "webhook-inbox": [
        field("inbound_secret", "Inbound shared secret", "secret", required=True,
              keychain_service="termite.webhook-inbox"),
        field("listen_port", "Listen port", "integer", required=True, default=8787),
        field("callback_url", "Reply callback URL",
              placeholder="https://example.com/termite-replies"),
        field("callback_bearer_token", "Callback bearer token", "secret",
              keychain_service="termite.webhook-inbox.callback"),
    ],
}


def entry_for(slug: str) -> dict:
    directory = ROOT / "plugins" / slug
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    version = manifest["version"]
    archive = ROOT / "dist" / f"{slug}-{version}.zip"
    if not archive.is_file():
        raise SystemExit(f"missing archive: {archive.relative_to(ROOT)}")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    mode, setup = CHANNEL_METADATA[slug]
    return {
        "kind": "channel",
        "id": manifest["id"],
        "name": manifest["name"].removesuffix(" Channel"),
        "description": manifest["description"],
        "mode": mode,
        "setup": setup,
        "configuration": {
            "version": 1,
            "fields": CHANNEL_CONFIGURATION[slug],
        },
        "author": "termite",
        "license": "MIT",
        "version": version,
        "homepage": manifest["homepage"],
        "file": f"dist/{slug}-{version}.zip",
        "sha256": digest,
        "sdk": "v1",
        "capabilities": manifest["capabilities"],
    }


def main() -> None:
    present = {
        path.parent.name for path in (ROOT / "plugins").glob("*/manifest.json")
        if "channels" in json.loads(path.read_text(encoding="utf-8")).get("capabilities", [])
    }
    expected = set(CHANNEL_METADATA)
    if set(CHANNEL_CONFIGURATION) != expected:
        raise SystemExit("Channel configuration metadata does not match the catalog")
    if present != expected:
        missing = sorted(expected - present)
        unknown = sorted(present - expected)
        raise SystemExit(f"Channel catalog mismatch; missing={missing}, unknown={unknown}")

    registry = json.loads(REGISTRY.read_text(encoding="utf-8"))
    retained = [
        entry for entry in registry["entries"]
        if not (entry.get("kind") == "channel" and str(entry.get("id", "")).startswith("dev.termite."))
    ]
    insertion = next((index for index, entry in enumerate(retained) if entry.get("kind") == "plugin"), len(retained))
    channels = [entry_for(slug) for slug in sorted(CHANNEL_METADATA)]
    registry["entries"] = retained[:insertion] + channels + retained[insertion:]
    ids = {entry["id"] for entry in registry["entries"]}
    registry["featured"] = [item for item in registry["featured"] if item in ids]
    REGISTRY.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    print(f"synced {len(channels)} first-party Channels")


if __name__ == "__main__":
    main()
