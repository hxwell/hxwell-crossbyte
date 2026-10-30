"""Small independent RFC6455 wire client for adapter integration tests."""
import base64
import hashlib
import os
import socket
import struct


def frame(opcode, payload=b"", final=True, masked=True):
    if isinstance(payload, str):
        payload = payload.encode()
    size = len(payload)
    head = bytes([(128 if final else 0) | opcode])
    bit = 128 if masked else 0
    if size < 126:
        head += bytes([bit | size])
    elif size <= 65535:
        head += bytes([bit | 126]) + struct.pack("!H", size)
    else:
        head += bytes([bit | 127]) + struct.pack("!Q", size)
    if not masked:
        return head + payload
    mask = os.urandom(4)
    return head + mask + bytes(value ^ mask[index % 4] for index, value in enumerate(payload))


class WebSocket:
    def __init__(self, port, path="/ws", headers=None, tls=None, prefetched=b""):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=8)
        if tls:
            self.socket = tls.wrap_socket(self.socket, server_hostname="localhost")
        self.reader = self.socket.makefile("rb")
        key = base64.b64encode(os.urandom(16)).decode()
        fields = {"Host": "localhost", "Connection": "Upgrade", "Upgrade": "websocket",
                  "Sec-WebSocket-Version": "13", "Sec-WebSocket-Key": key, **(headers or {})}
        request = f"GET {path} HTTP/1.1\r\n" + "".join(f"{name}: {value}\r\n" for name, value in fields.items()) + "\r\n"
        self.socket.sendall(request.encode() + prefetched)
        line = self.reader.readline()
        if not line.startswith(b"HTTP/1.1 101 "):
            self.close_transport()
            raise AssertionError(f"WebSocket upgrade failed: {line!r}")
        self.headers = []
        while True:
            line = self.reader.readline()
            if line == b"\r\n":
                break
            if not line:
                raise AssertionError("EOF in upgrade response")
            name, value = line.decode("latin-1").split(":", 1)
            self.headers.append((name.lower(), value.strip()))
        accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        assert ("sec-websocket-accept", accept) in self.headers

    def send(self, value, binary=False):
        self.socket.sendall(frame(2 if binary else 1, value))

    def receive(self):
        head = self.reader.read(2)
        if len(head) != 2:
            raise AssertionError("WebSocket EOF")
        opcode, size = head[0] & 15, head[1] & 127
        self.final = bool(head[0] & 128)
        assert not (head[1] & 128), "server frame must be unmasked"
        if size == 126:
            size = struct.unpack("!H", self.reader.read(2))[0]
        elif size == 127:
            size = struct.unpack("!Q", self.reader.read(8))[0]
        payload = self.reader.read(size)
        assert len(payload) == size
        return opcode, payload

    def message(self):
        opcode, body = self.receive()
        while not self.final:
            next_opcode, part = self.receive()
            assert next_opcode == 0
            body += part
        return body.decode() if opcode == 1 else body

    def close_transport(self):
        self.reader.close()
        self.socket.close()

    def close(self):
        try:
            self.socket.sendall(frame(8, struct.pack("!H", 1000)))
            while self.receive()[0] != 8:
                pass
        except (OSError, AssertionError):
            pass
        finally:
            self.close_transport()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
