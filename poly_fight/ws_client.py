"""Minimal stdlib WebSocket client (RFC 6455). No third-party deps.

Used by the on-chain follow detector to consume Polygon `eth_subscribe` log
streams, and reusable for the Polymarket user channel later. Implements only
what we need: TLS connect, HTTP Upgrade handshake, masked client frames, frame
decode with fragmentation + control-frame (PING/PONG/CLOSE) handling.

Framing stays blocking for stream integrity (a socket timeout mid-frame would
desync the byte stream). Callers bound their loop by checking a deadline between
complete messages; Polygon newHeads arrive ~every 2s so recv never blocks long.
No extensions/compression, no auto-reconnect (callers handle reconnect).
"""
from __future__ import annotations

import base64
import os
import socket
import ssl
import struct
from urllib.parse import urlparse


class WSError(RuntimeError):
    pass


class WSTimeout(Exception):
    pass


class WSClient:
    OP_CONT = 0x0
    OP_TEXT = 0x1
    OP_BIN = 0x2
    OP_CLOSE = 0x8
    OP_PING = 0x9
    OP_PONG = 0xA

    def __init__(self, url: str, *, connect_timeout: float = 30.0):
        self.url = url
        self.connect_timeout = connect_timeout
        self.sock: ssl.SSLSocket | socket.socket | None = None
        self._buf = b""

    # -- connection ------------------------------------------------------- #
    def connect(self) -> None:
        u = urlparse(self.url)
        host = u.hostname
        port = u.port or (443 if u.scheme == "wss" else 80)
        raw = socket.create_connection((host, port), timeout=self.connect_timeout)
        if u.scheme == "wss":
            ctx = ssl.create_default_context()
            raw = ctx.wrap_socket(raw, server_hostname=host)
        self.sock = raw
        key = base64.b64encode(os.urandom(16)).decode()
        path = (u.path or "/") + (f"?{u.query}" if u.query else "")
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(req.encode())
        head = self._read_handshake()
        status = head.split(b"\r\n", 1)[0]
        if b"101" not in status:
            raise WSError(f"handshake failed: {status.decode(errors='replace')}")

    def _read_handshake(self) -> bytes:
        while b"\r\n\r\n" not in self._buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise WSError("connection closed during handshake")
            self._buf += chunk
        idx = self._buf.index(b"\r\n\r\n") + 4
        head, self._buf = self._buf[:idx], self._buf[idx:]
        return head

    def set_timeout(self, seconds: float | None) -> None:
        """Socket read timeout. recv_message stays frame-safe across timeouts."""
        if self.sock:
            self.sock.settimeout(seconds)

    # -- low-level framing ------------------------------------------------ #
    def _fill(self, n: int) -> None:
        """Ensure at least n bytes are buffered, WITHOUT consuming them.

        On socket timeout the buffer is left intact, so a timeout that fires
        part-way through a frame doesn't lose bytes — the next recv_message
        resumes cleanly. This is what makes a polling/stop-check loop safe.
        """
        while len(self._buf) < n:
            try:
                chunk = self.sock.recv(65536)
            except (socket.timeout, ssl.SSLWantReadError):
                raise WSTimeout()
            if not chunk:
                raise WSError("connection closed")
            self._buf += chunk

    def _take(self, n: int) -> bytes:
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        header = bytearray([0x80 | opcode])  # FIN set, single frame
        n = len(payload)
        if n < 126:
            header.append(0x80 | n)  # mask bit set (client MUST mask)
        elif n < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", n)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", n)
        mask = os.urandom(4)
        header += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + masked)

    def send_text(self, text: str) -> None:
        self._send_frame(self.OP_TEXT, text.encode())

    def recv_message(self) -> tuple[int, bytes]:
        """Return (opcode, payload) for one complete data message.

        Transparently answers PINGs with PONGs and reassembles fragments.
        Raises WSTimeout if the socket timeout elapses mid-read, WSError on close.
        """
        frags = bytearray()
        msg_opcode = None
        while True:
            # Peek the whole frame into the buffer before consuming any of it,
            # so a WSTimeout mid-frame leaves the byte stream intact.
            self._fill(2)
            b0, b1 = self._buf[0], self._buf[1]
            masked = b1 & 0x80
            length = b1 & 0x7F
            offset = 2
            if length == 126:
                self._fill(4)
                length = struct.unpack(">H", self._buf[2:4])[0]
                offset = 4
            elif length == 127:
                self._fill(10)
                length = struct.unpack(">Q", self._buf[2:10])[0]
                offset = 10
            mask_len = 4 if masked else 0
            self._fill(offset + mask_len + length)
            self._take(offset)
            mask = self._take(mask_len) if masked else None
            payload = self._take(length) if length else b""
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            fin = b0 & 0x80
            opcode = b0 & 0x0F

            if opcode == self.OP_PING:
                self._send_frame(self.OP_PONG, payload)
                continue
            if opcode == self.OP_PONG:
                continue
            if opcode == self.OP_CLOSE:
                return (self.OP_CLOSE, payload)
            if opcode == self.OP_CONT:
                frags += payload
            else:
                msg_opcode = opcode
                frags = bytearray(payload)
            if fin:
                return (msg_opcode, bytes(frags))

    def close(self) -> None:
        try:
            if self.sock:
                self._send_frame(self.OP_CLOSE, b"")
        except Exception:  # noqa: BLE001
            pass
        finally:
            if self.sock:
                try:
                    self.sock.close()
                except Exception:  # noqa: BLE001
                    pass
            self.sock = None
