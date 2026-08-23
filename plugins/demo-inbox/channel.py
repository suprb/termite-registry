#!/usr/bin/env python3
"""A local, credential-free connector that exercises the complete Channel loop."""

import json
import os
import time
import urllib.request


BASE_URL = f"http://127.0.0.1:{os.environ['TERMITE_PORT']}"
HEADERS = {
    "Authorization": f"Bearer {os.environ['TERMITE_TOKEN']}",
    "Content-Type": "application/json",
}
CHANNEL_ID = "dev.termite.demo-inbox.inbox"


def request(path, body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    method = "GET" if body is None else "POST"
    outgoing = urllib.request.Request(
        BASE_URL + path, data=data, headers=HEADERS, method=method
    )
    with urllib.request.urlopen(outgoing, timeout=30) as response:
        payload = response.read()
    return json.loads(payload) if payload else {}


def deliver(reply):
    # A real connector calls its provider's send-message API here. Printing is
    # the Demo provider, so acknowledging after the write is truthful.
    request(f"/v1/channel-replies/{reply['id']}/attempt", {})
    print(f"outbound [{reply['conversationID']}]: {reply['body']}", flush=True)
    request(f"/v1/channel-replies/{reply['id']}/ack", {"delivered": True})


registration = request("/v1/channels", {
    "id": CHANNEL_ID,
    "name": "Demo Inbox",
    "service": "Demo",
    "account": "local",
    "description": "A complete Receive / Route / Reply Channel",
    "replyCapabilities": ["reply"],
})

for pending in registration.get("pendingReplies", []):
    deliver(pending)

request(f"/v1/channels/{CHANNEL_ID}/work-items", {
    "id": "welcome",
    "deliveryID": "demo-welcome-v1",
    "conversationID": "demo-thread",
    "senderID": "demo-user",
    "senderName": "Demo Channel",
    "title": "Try the Work Inbox",
    "body": "Open Channels > Work Inbox. Start an agent, stage a shell command, or reply.",
})

request(f"/v1/channels/{CHANNEL_ID}/health", {
    "status": "healthy",
    "lastSuccessAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "detail": "Local Demo provider is ready",
})

stream = urllib.request.Request(BASE_URL + "/v1/events", headers=HEADERS)
with urllib.request.urlopen(stream) as events:
    for raw in events:
        if not raw.startswith(b"data: "):
            continue
        event = json.loads(raw[6:])
        if event.get("kind") == "channel-reply":
            deliver(event)
