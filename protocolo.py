"""Protocolo TCP: un objeto JSON UTF-8 por línea (JSON Lines)."""
from __future__ import annotations

import json
import re
import socket
import threading
from typing import Any, BinaryIO

MAX_MESSAGE_BYTES = 65536
NODE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


def valid_node_id(value: object) -> bool:
    return isinstance(value, str) and bool(NODE_ID_PATTERN.fullmatch(value))


def send_json(sock: socket.socket, message: dict[str, Any], lock: threading.Lock | None = None) -> None:
    payload = (json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    if len(payload) > MAX_MESSAGE_BYTES:
        raise ValueError("Mensaje TCP demasiado grande")
    if lock is None:
        sock.sendall(payload)
    else:
        with lock:
            sock.sendall(payload)


def recv_json(file: BinaryIO) -> dict[str, Any] | None:
    line = file.readline(MAX_MESSAGE_BYTES + 1)
    if not line:
        return None  # conexión cerrada
    if len(line) > MAX_MESSAGE_BYTES or not line.endswith(b"\n"):
        raise ValueError("Trama TCP demasiado grande o sin delimitador de línea")
    decoded = json.loads(line.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("La trama debe ser un objeto JSON")
    return decoded
