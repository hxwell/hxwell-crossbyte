import concurrent.futures
import contextlib
import json
import socket
import ssl
import struct
import time
import zlib

from ws_client import WebSocket, frame


def run_features(test, check, request, header, cookie_header, raw_exchange, port, tls_port, cert, target):
    def routing_and_views():
        check(request(port, "/feature/optional")[2] == b"hello:anon")
        check(request(port, "/feature/optional/Ada")[2] == b"hello:Ada")
        check(request(port, "/feature/number/123")[2] == b"123")
        check(request(port, "/feature/number/letters")[0] == 404)
        status, fields, _ = request(port, "/feature/named-redirect")
        check(status == 302 and header(fields, "location") == "/feature/named")
        check(request(port, "/tenant", headers={"Host": "tenant.example"})[2] == b"tenant")
        check(request(port, "/tenant", headers={"Host": "other.example"})[0] == 404)
        check(request(port, "/feature/file")[2] == b"static file works")
        status, fields, body = request(port, "/feature/middleware")
        check(status == 200 and body == b"route" and header(fields, "x-middleware") == "before" and header(fields, "x-after") == "after")
        status, _, body = request(port, "/feature/json-status")
        check(status == 201 and json.loads(body) == {"created": True, "nested": {"value": "nested"}})
        check(request(port, "/feature/method/validated")[0] == 405)
        check(request(port, "/feature/method/validated", "POST", b"", {"Content-Type": "application/json"})[0] == 422)
        check(request(port, "/feature/method/validated", "POST", b'{"field":"yes"}', {"Content-Type": "application/json"})[2] == b"valid:yes")
        for verb in ("PUT", "PATCH", "DELETE", "OPTIONS"):
            check(request(port, "/feature/method/verbs", verb)[2] == verb.encode())
        def render(index):
            check(request(port, f"/feature/template/owner-{index}")[2] == f"Hello owner-{index}!".encode())
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(render, range(48)))
    test("route groups/domains/constraints/names, verbs/validation, nested JSON, file and concurrent templates", routing_and_views)

    def services():
        status, _, body = request(port, "/feature/services")
        check(status == 200, body)
        data = json.loads(body)
        check(data == {"cache": "cached", "scheduled": "done", "decrypt": "private value", "checked": True,
                       "rejected": True, "command": "command:argument"}, data)
    test("cache, encryption/hash, scheduled work and command dispatch from a request", services)

    def authentication():
        status, fields, _ = request(port, "/feature/protected")
        check(status == 302 and header(fields, "location") == "/feature/login-page")
        _, fields, _ = request(port, "/feature/login/default/member")
        member_cookie, _ = cookie_header(fields)
        check(json.loads(request(port, "/feature/who/default", headers={"Cookie": member_cookie})[2]) == {"id": "member", "user": "member", "checked": True})
        check(json.loads(request(port, "/feature/who/admin", headers={"Cookie": member_cookie})[2]) == {"id": None, "user": None, "checked": False})
        check(request(port, "/feature/protected", headers={"Cookie": member_cookie})[2] == b"protected")
        _, fields, _ = request(port, "/feature/login/admin/administrator", headers={"Cookie": member_cookie})
        both_cookie, _ = cookie_header(fields)
        check(json.loads(request(port, "/feature/who/default", headers={"Cookie": both_cookie})[2])["id"] == "member")
        check(json.loads(request(port, "/feature/who/admin", headers={"Cookie": both_cookie})[2])["id"] == "administrator")
        check(json.loads(request(port, "/feature/who/default", headers={"Cookie": member_cookie})[2])["checked"] is False)
        check(request(port, "/feature/logout", headers={"Cookie": both_cookie})[0] == 200)
        check(json.loads(request(port, "/feature/who/admin", headers={"Cookie": both_cookie})[2])["checked"] is False)
    test("authentication guards, RequireAuth, session regeneration and logout replay", authentication)

    def explicit_encoding():
        expected = b"encoded-data!" * 1000
        lengths = []
        for level in (0, 9):
            status, fields, body = request(port, f"/feature/encoding/{level}", headers={"Accept-Encoding": "deflate"})
            check(status == 200 and header(fields, "content-encoding") == "deflate")
            check(zlib.decompress(body) == expected)
            lengths.append(len(body))
        check(lengths[0] > lengths[1] * 2, lengths)
        for path, accept in [("empty", "gzip, deflate"), ("9", "gzip, deflate;q=0"), ("9", "*;q=1, deflate;q=0")]:
            status, fields, body = request(port, f"/feature/encoding/{path}", headers={"Accept-Encoding": accept})
            check(status == 200 and header(fields, "content-encoding") is None and body == expected)
    test("HxWell Deflate/Empty encoding options, levels/chunks and q=0 negotiation", explicit_encoding)

    def admission():
        sockets = []
        try:
            for _ in range(32):
                sock = socket.create_connection(("127.0.0.1", port), timeout=8)
                reader = sock.makefile("rb")
                sockets.append((sock, reader))
                sock.sendall(b"GET /feature/hold HTTP/1.1\r\nHost: localhost\r\n\r\n")
                # Acknowledge admission before opening the next holder. Racing
                # a probe against the last holder can refuse the holder itself,
                # leaving 31 held slots and allowing every subsequent probe.
                check(reader.readline().startswith(b"HTTP/1.1 200 "))
                while True:
                    field = reader.readline()
                    check(field, "holder disconnected before headers")
                    if field == b"\r\n":
                        break
                size = int(reader.readline(), 16)
                check(reader.read(size) == b"held" and reader.read(2) == b"\r\n")
            check(request(port, "/hello/overflow")[0] == 503, "admission cap was not enforced")
        finally:
            for sock, reader in sockets:
                reader.close()
                sock.close()
        deadline = time.monotonic() + 3
        while request(port, "/hello/recovered")[0] != 200:
            check(time.monotonic() < deadline, "deferred client disconnects leaked admission slots")
            time.sleep(0.02)
    test("bounded deferred-request admission and slot release after disconnect", admission)

    def websocket_features():
        with WebSocket(port, "/ws?name=owner") as ws:
            ws.send("query")
            check(ws.message() == "query:owner")
            ws.send("thread")
            check(ws.message() == ("virtual:true" if target == "jvm" else "platform"))
            ws.send("callback")
            check(ws.message() == "first" and ws.message() == "callback")
            ws.send("error")
            check(ws.message() == "handled-error")
            binary = bytes(range(256)) * 512
            ws.send(binary, binary=True)
            check(ws.message() == binary)
        with WebSocket(port) as first, WebSocket(port) as second:
            first.send("broadcast:all-clients")
            check(first.message() == "all-clients" and second.message() == "all-clients")
        denied = raw_exchange(port, b"GET /ws-guarded HTTP/1.1\r\nHost: localhost\r\nConnection: close, upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
        check(denied.startswith(b"HTTP/1.1 403 "))
        denied = raw_exchange(port, b"GET /ws-auth HTTP/1.1\r\nHost: localhost\r\nConnection: close, upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
        check(denied.startswith(b"HTTP/1.1 302 "))
        _, fields, _ = request(port, "/feature/login/default/ws-member")
        cookie, _ = cookie_header(fields)
        with WebSocket(port, "/ws-auth", {"Cookie": cookie}) as ws:
            ws.send("authorized")
            check(ws.message() == "echo:authorized")
        if tls_port:
            context = ssl.create_default_context(cafile=str(cert))
            context.set_alpn_protocols(["http/1.1"])
            with WebSocket(tls_port, tls=context) as ws:
                ws.send("secure")
                check(ws.message() == "echo:secure")
        # More handoffs than the native HTTP connection cap: leaked HTTP slots
        # would refuse the later upgrades even though the WS peers all closed.
        for _ in range(140):
            with WebSocket(port):
                pass
        check(request(port, "/hello/after-ws-churn")[0] == 200)
        with WebSocket(port) as ws:
            ws.socket.sendall(b"\x82\xff" + struct.pack("!Q", 1024 * 1024 + 1) + b"mask")
            opcode, payload = ws.receive()
            check(opcode == 8 and int.from_bytes(payload[:2], "big") == 1009)
        with WebSocket(port) as ws:
            ws.socket.sendall(frame(1, "block") + frame(1, "burst") * 140)
            opcode, payload = ws.receive()
            check(opcode == 8 and int.from_bytes(payload[:2], "big") == 1013)
        with WebSocket(port) as ws:
            ws.send("outbound-overflow")
            opcode, payload = ws.receive()
            check(opcode == 8 and int.from_bytes(payload[:2], "big") == 1013)
    test("WebSocket binary/broadcast/callback/error, query state, authorization" + (" and WSS" if tls_port else ""), websocket_features)

    def socket_io():
        def opened(ws):
            check(ws.message().startswith("0"))
            check(ws.message().startswith("40"))
            check(json.loads(ws.message()[2:]) == ["welcome", "connected"])
        with WebSocket(port, "/socket.io?EIO=4&transport=websocket") as first, WebSocket(port, "/socket.io?EIO=4&transport=websocket") as second:
            opened(first)
            opened(second)
            first.send('42["echo",{"value":"works"}]')
            check(json.loads(first.message()[2:]) == ["reply", {"value": "works"}])
            first.send("2")
            check(first.message() == "3")
            first.send('42["room","lobby-message"]')
            check(json.loads(first.message()[2:]) == ["room-reply", "lobby-message"])
            check(json.loads(second.message()[2:]) == ["room-reply", "lobby-message"])
        status, _, body = request(port, "/socket.io?EIO=4&transport=polling")
        check(status == 200 and body.startswith(b"0"))
        sid = json.loads(body[1:])["sid"]
        check(request(port, "/socket.io?transport=polling&sid=" + sid)[2] == b"6")
        check(request(port, "/socket.io?transport=polling&sid=" + sid, "POST", b"2")[2] == b"ok")
        with WebSocket(port, "/socket.io?EIO=4&transport=websocket&sid=" + sid) as upgraded:
            upgraded.send("2probe")
            check(upgraded.message() == "3probe")
            upgraded.send("5")
            check(upgraded.message().startswith("40"))
            check(json.loads(upgraded.message()[2:]) == ["welcome", "connected"])
    test("Socket.IO events, rooms, ping and polling-to-WebSocket upgrade", socket_io)

    if target == "neko":
        def database():
            status, _, body = request(port, "/db/exercise")
            check(status == 200, body)
            result = json.loads(body)
            check(result["count"] == 1 and result["name"] == "updated" and result["index"])
            check(set(result["columns"]) == {"id", "name", "extra"})
            check(result["model"]["name"] == "updated")
            rows = json.loads(request(port, "/db/rows")[2])
            check(len(rows) == 1 and rows[0]["name"] == "updated")
            for password, expected in [("wrong", False), ("correct", True)]:
                status, fields, body = request(port, "/db/login", "POST", json.dumps({"username": "tester", "password": password}), {"Content-Type": "application/json"})
                check(status == 200 and json.loads(body)["loggedIn"] is expected)
                if expected:
                    cookie, _ = cookie_header(fields)
                    data = json.loads(request(port, "/feature/who/database", headers={"Cookie": cookie})[2])
                    check(data["checked"] and data["id"] == data["user"] == 1)
            for _ in range(25):
                with WebSocket(port) as ws:
                    ws.send("database")
                    check(ws.message() == "database:1")
        test("live SQLite schema/ORM/transactions/model/result-set streaming and Auth.attempt", database)
