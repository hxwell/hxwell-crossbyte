#!/usr/bin/env python3
"""Compile a real HxWell application and test the Crossbyte transport over TCP.

Requires Haxe 4.3.7 or 5.0.0-preview.1, Java 21+, the HxWell/Crossbyte/Emitter source checkouts, and
tests/requirements.txt. All generated files and session state stay in out/.
"""
import argparse
import concurrent.futures
import contextlib
import gzip
import hashlib
import http.client
import http.cookies
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.parse
import zlib
from ws_client import WebSocket, frame
from features import run_features
from environment import local_libraries, revision

import h2.config
import h2.connection
import h2.events

ROOT = Path(__file__).resolve().parents[1]
RESULTS = []


def check(condition, message="check failed"):
    if not condition:
        raise AssertionError(message)


def request(port, path, method="GET", body=None, headers=None, connection=None):
    own = connection is None
    conn = connection or http.client.HTTPConnection("127.0.0.1", port, timeout=8)
    try:
        conn.request(method, path, body, headers or {})
        response = conn.getresponse()
        return response.status, response.getheaders(), response.read()
    finally:
        if own:
            conn.close()


def header(fields, name):
    return next((value for key, value in fields if key.lower() == name.lower()), None)


def cookie_header(fields):
    cookies = http.cookies.SimpleCookie()
    for key, value in fields:
        if key.lower() == "set-cookie":
            cookies.load(value)
    return "; ".join(f"{key}={value.value}" for key, value in cookies.items()), cookies


def raw_exchange(port, wire, half_close=False):
    with socket.create_connection(("127.0.0.1", port), timeout=8) as sock:
        sock.sendall(wire)
        if half_close:
            sock.shutdown(socket.SHUT_WR)
        result = bytearray()
        while True:
            try:
                part = sock.recv(65536)
            except ConnectionResetError:
                break
            if not part:
                break
            result.extend(part)
            check(len(result) < 8 * 1024 * 1024, "unexpected raw response length")
        return bytes(result)


def read_wire_response(reader, slow=False, head_only=False):
    line = reader.readline()
    check(line.startswith(b"HTTP/1.1 "), repr(line))
    status = int(line.split()[1])
    fields = []
    while True:
        line = reader.readline()
        check(line, "EOF within HTTP headers")
        if line == b"\r\n":
            break
        key, value = line.decode("latin-1").split(":", 1)
        fields.append((key, value.strip()))
    body = bytearray()
    if head_only or status in (204, 304):
        return status, fields, b""
    if header(fields, "transfer-encoding") == "chunked":
        while True:
            line = reader.readline()
            check(line, "EOF before last chunk")
            size = int(line.split(b";", 1)[0], 16)
            if not size:
                while True:
                    trailer = reader.readline()
                    check(trailer, "EOF in chunk trailers")
                    if trailer == b"\r\n":
                        break
                break
            left = size
            while left:
                part = reader.read(min(left, 4096 if slow else left))
                check(part, "truncated chunk")
                body.extend(part)
                left -= len(part)
                if slow:
                    time.sleep(0.0003)
            check(reader.read(2) == b"\r\n", "chunk delimiter")
    else:
        length = int(header(fields, "content-length") or 0)
        body.extend(reader.read(length))
        check(len(body) == length, "truncated response")
    return status, fields, bytes(body)


def h2_request(port, paths, tls_context=None):
    with socket.create_connection(("127.0.0.1", port), timeout=8) as tcp:
        with contextlib.ExitStack() as stack:
            sock = tcp
            if tls_context:
                sock = stack.enter_context(tls_context.wrap_socket(tcp, server_hostname="localhost"))
                check(sock.selected_alpn_protocol() == "h2", "TLS ALPN did not negotiate h2")
            conn = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True, header_encoding="utf-8"))
            conn.initiate_connection()
            results = {}
            for index, path in enumerate(paths):
                stream = index * 2 + 1
                results[stream] = {"headers": [], "body": bytearray(), "ended": False}
                conn.send_headers(stream, [(":method", "GET"), (":scheme", "https" if tls_context else "http"),
                                          (":authority", "localhost"), (":path", path)], end_stream=True)
            sock.sendall(conn.data_to_send())
            while not all(item["ended"] for item in results.values()):
                data = sock.recv(65536)
                check(data, "HTTP/2 connection ended early")
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.ResponseReceived):
                        results[event.stream_id]["headers"] = event.headers
                    elif isinstance(event, h2.events.DataReceived):
                        results[event.stream_id]["body"].extend(event.data)
                        conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                    elif isinstance(event, h2.events.StreamEnded):
                        results[event.stream_id]["ended"] = True
                    elif isinstance(event, (h2.events.StreamReset, h2.events.ConnectionTerminated)):
                        raise AssertionError(f"HTTP/2 failure: {event}")
                output = conn.data_to_send()
                if output:
                    sock.sendall(output)
            return list(results.values())


def suite(port, tls_port, cert, target):
    def test(name, work):
        started = time.monotonic()
        try:
            work()
        except Exception as error:
            RESULTS.append({"name": name, "passed": False, "error": str(error)})
            raise
        RESULTS.append({"name": name, "passed": True, "seconds": round(time.monotonic() - started, 3)})
        print("PASS", name, flush=True)

    def routing():
        status, fields, body = request(port, "/hello/Ada?q=a%3Db%252F", headers={"X-Custom": "works", "Cookie": "custom=yes"})
        data = json.loads(body)
        check(status == 200 and header(fields, "content-type") == "application/json")
        check(data["name"] == "Ada" and data["query"] == "a=b%2F" and data["cookie"] == "yes")
        check(data["header"] == "works" and data["virtualThread"] is (target == "jvm") and data["protocol"] == "HTTP/1.1")
        check(data["remote"] == "127.0.0.1")
    test("routing, headers, cookies, single query decode and request worker", routing)

    def bodies():
        for payload, content_type, expected in [(b'{"field":"works"}', "application/json", "works"),
                                                (b"field=a%2Bb", "application/x-www-form-urlencoded", "a+b")]:
            status, _, body = request(port, "/echo", "POST", payload, {"Content-Type": content_type})
            data = json.loads(body)
            check(status == 200 and data["field"] == expected and data["body"] == payload.decode())
        payload = b'{"field":"compressed"}'
        for encoding, encoded in [("gzip", gzip.compress(payload)), ("deflate", zlib.compress(payload))]:
            status, _, body = request(port, "/echo", "POST", encoded,
                                      {"Content-Type": "application/json", "Content-Encoding": encoding})
            data = json.loads(body)
            check(status == 200 and data["field"] == "compressed" and data["body"] == payload.decode())
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=8)
        try:
            conn.request("POST", "/echo", iter([b'{"field":', b'"chunked"}']), {"Content-Type": "application/json"}, encode_chunked=True)
            response = conn.getresponse()
            check(response.status == 200 and json.loads(response.read())["field"] == "chunked")
            status, _, body = request(port, "/hello/next", connection=conn)
            check(status == 200 and json.loads(body)["name"] == "next")
        finally:
            conn.close()
        binary = bytes(range(256)) * 8
        check(request(port, "/upload", "POST", binary)[2] == binary)
    test("JSON/form, gzip/deflate/chunked requests, bounded request Input", bodies)

    def headers_and_sessions():
        status, fields, body = request(port, "/headers")
        check(status == 201 and body == b"headers")
        check(header(fields, "x-base") == "static" and header(fields, "x-reply") == "hxwell-crossbyte")
        check(header(fields, "content-type") == "text/html")
        _, cookies = cookie_header(fields)
        check(cookies["first"].value == "1" and cookies["second"].value == "2")
        check(cookies["second"]["path"] == "/app")
        status, fields, _ = request(port, "/session/set/persisted")
        cookie, _ = cookie_header(fields)
        check(status == 200 and request(port, "/session/get", headers={"Cookie": cookie})[2] == b"persisted")
        check(request(port, "/session/get")[2] == b"none")
        status, fields, _ = request(port, "/csrf")
        cookie, cookies = cookie_header(fields)
        check(status == 200 and "XSRF-TOKEN" in cookies)
        check(request(port, "/csrf", "POST", b"", {"Cookie": cookie})[0] == 419)
        token = urllib.parse.unquote(cookies["XSRF-TOKEN"].value)
        check(request(port, "/csrf", "POST", b"", {"Cookie": cookie, "X-XSRF-TOKEN": token})[2] == b"csrf-ok")
    test("response metadata, multiple Set-Cookie, sessions and CSRF", headers_and_sessions)

    def statuses():
        check(request(port, "/missing")[0] == 404)
        check(request(port, "/hello/Ada", "POST")[0] == 405)
        check(request(port, "/abort/403")[0] == 403)
        check(request(port, "/hello.txt")[2] == b"static file works")
        check(request(port, "/.secret")[0] in (403, 404))
        check(request(port, "/escape.txt")[0] == 403)
        check(request(port, "/manual")[2] == b"first-second!")
        check(request(port, "/headers", "HEAD")[0] == 405)
        status, _, body = request(port, "/hello.txt", "HEAD")
        check(status == 200 and body == b"")
        check(request(port, "/abort/204")[2] == b"")
        check(request(port, "/abort/304")[2] == b"")
    test("routing errors, public-file security, HEAD, 204/304, manual output", statuses)

    def deferred_and_parallel():
        def worker(index):
            status, fields, body = request(port, f"/deferred/{index}")
            check(status == 200 and header(fields, "x-owner") == str(index) and body == f"deferred-{index}".encode())
            data = json.loads(request(port, f"/hello/user-{index}?q=private-{index}")[2])
            check(data["name"] == f"user-{index}" and data["query"] == f"private-{index}" and data["virtualThread"] is (target == "jvm"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(worker, range(64)))
    test("128 concurrent/deferred HTTP exchanges keep request state isolated", deferred_and_parallel)

    def pipelining():
        with socket.create_connection(("127.0.0.1", port), timeout=8) as sock:
            sock.sendall(b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nDATA"
                         b"GET /hello/pipelined HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            with sock.makefile("rb") as reader:
                first = read_wire_response(reader)
                second = read_wire_response(reader)
                check(first[0] == 200 and first[2] == b"DATA")
                check(second[0] == 200 and json.loads(second[2])["name"] == "pipelined")
    test("pipelined POST then GET keeps body and response boundaries", pipelining)

    def head_pipeline():
        with socket.create_connection(("127.0.0.1", port), timeout=8) as sock:
            sock.sendall(b"HEAD /feature/encoding/empty HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n\r\n"
                         b"GET /compress HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n")
            with sock.makefile("rb") as reader:
                first = read_wire_response(reader, head_only=True)
                second = read_wire_response(reader)
                check(first[0] == 200 and first[2] == b"")
                check(second[0] == 200 and header(second[1], "content-encoding") == "gzip")
                check(gzip.decompress(second[2]) == b"crossbyte-compression!" * 1000)
    test("pipelined HEAD cannot change the next request's encoding negotiation", head_pipeline)

    def framing():
        cases = [b"Content-Length: 4\r\nTransfer-Encoding: chunked", b"Content-Length: 4\r\ncontent-length: 0",
                 b"Content-Length: 4\r\nContent-Length: 4",
                 b"Content-Length: 0x4", b"Content-Length: +4", b"Content-Length: 4junk", b"Content-Length: 4294967300",
                 b"Content-Length : 4", b"Content-Length:\x0b4", b"Content-Length:\x0c4"]
        smuggled = b"2e\r\nABCDGET /hello/smuggled HTTP/1.1\r\nHost: localhost\r\n\r\n\r\n0\r\n\r\n"
        for fields in cases:
            response = raw_exchange(port, b"POST /echo HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n" + fields + b"\r\n\r\n" + smuggled)
            check(response.startswith(b"HTTP/1.1 400 "), (fields, response[:200]))
            check(b"HTTP/1.1 200" not in response)
        for wire in [b"GET /hello/bad HTTP/1.1\nHost: localhost\nConnection: close\n\n",
                     b"GET /hello/bad HTTP/1.1 surplus\r\nHost: localhost\r\nConnection: close\r\n\r\n"]:
            check(raw_exchange(port, wire).startswith(b"HTTP/1.1 400 "))
        for path in (b"/echo", b"/upload"):
            response = raw_exchange(port, b"POST " + path + b" HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1048577\r\nConnection: close\r\n\r\n")
            check(response.startswith(b"HTTP/1.1 413 "))
        check(request(port, "/hello/x?q=" + "a" * 1024)[0] == 414)
        check(request(port, "/hello/x", headers={"X-Large": "a" * 8192})[0] == 431)
        path = "/redirect?to=%2Fsafe%0D%0ASet-Cookie%3Ainjected%3D1"
        status, fields, _ = request(port, path)
        check(status == 500 and not any("injected" in value for _, value in fields))
    test("framing/smuggling, limits and CRLF rejection", framing)

    def websockets():
        with WebSocket(port) as ws:
            ws.send("hello")
            check(ws.message() == "echo:hello")
            ws.socket.sendall(frame(9, b"ping"))
            check(ws.receive() == (10, b"ping"))
            ws.socket.sendall(frame(1, "split-", final=False) + frame(0, "message"))
            check(ws.message() == "echo:split-message")
        with WebSocket(port, prefetched=frame(1, "early")) as ws:
            check(ws.message() == "echo:early")
        for invalid, expected in [(frame(1, b"unmasked", masked=False), 1002), (frame(1, b"\xff"), 1007)]:
            with WebSocket(port) as ws:
                ws.socket.sendall(invalid)
                opcode, payload = ws.receive()
                check(opcode == 8 and int.from_bytes(payload[:2], "big") == expected)
    test("RFC6455 upgrade, text, ping, fragmentation, prefetched bytes and protocol validation", websockets)

    def streaming():
        with socket.create_connection(("127.0.0.1", port), timeout=8) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)
            sock.sendall(b"GET /large HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            time.sleep(0.15)
            started = time.monotonic()
            check(request(port, "/hello/responsive")[0] == 200)
            check(time.monotonic() - started < 2, "slow reader blocked the Crossbyte event loop")
            with sock.makefile("rb") as reader:
                status, _, body = read_wire_response(reader, slow=True)
        expected = bytes((index * 31 + 7) & 255 for index in range(4 * 1024 * 1024 + 137))
        check(status == 200 and hashlib.sha256(body).digest() == hashlib.sha256(expected).digest(), len(body))
        # Disconnect stalled producers; their admission slots must be released.
        for _ in range(40):
            with socket.create_connection(("127.0.0.1", port), timeout=8) as sock:
                sock.sendall(b"GET /large HTTP/1.1\r\nHost: localhost\r\n\r\n")
                sock.recv(1024)
        check(request(port, "/hello/after-disconnect")[0] == 200)
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=8)
        try:
            conn.request("GET", "/broken")
            response = conn.getresponse()
            try:
                response.read()
            except (http.client.IncompleteRead, ConnectionResetError):
                pass
            else:
                raise AssertionError("failed body source was incorrectly completed as a successful response")
        finally:
            conn.close()
    test("4 MiB streaming through 128 KiB output cap, backpressure and disconnect cleanup", streaming)

    def compression():
        expected = b"crossbyte-compression!" * 1000
        for encoding in ("gzip", "deflate"):
            status, fields, body = request(port, "/compress", headers={"Accept-Encoding": encoding})
            check(status == 200 and header(fields, "content-encoding") == encoding)
            decoded = gzip.decompress(body) if encoding == "gzip" else zlib.decompress(body)
            check(decoded == expected)
    test("Crossbyte response gzip and deflate compression", compression)

    def http2():
        paths = [f"/deferred/h2-{index}" for index in range(12)]
        for index, result in enumerate(h2_request(port, paths)):
            check(header(result["headers"], ":status") == "200")
            check(header(result["headers"], "x-owner") == f"h2-{index}" and result["body"] == f"deferred-h2-{index}".encode())
        result = h2_request(port, ["/hello/protocol"])[0]
        check(json.loads(result["body"])["protocol"] == "HTTP/2")
        result = h2_request(port, ["/large"])[0]
        check(len(result["body"]) == 4 * 1024 * 1024 + 137)
        check(all(value == (index * 31 + 7) & 255 for index, value in enumerate(result["body"])))
    test("HTTP/2 multiplexed deferred responses and flow-controlled large body", http2)

    def tls():
        context = ssl.create_default_context(cafile=str(cert))
        context.set_alpn_protocols(["http/1.1"])
        conn = http.client.HTTPSConnection("localhost", tls_port, timeout=8, context=context)
        try:
            status, _, body = request(tls_port, "/hello/tls", connection=conn)
            check(status == 200 and json.loads(body)["name"] == "tls")
        finally:
            conn.close()
        context.set_alpn_protocols(["h2", "http/1.1"])
        result = h2_request(tls_port, ["/hello/alpn"], context)[0]
        data = json.loads(result["body"])
        check(data["protocol"] == "HTTP/2" and data["name"] == "alpn")
    if tls_port:
        test("TLS certificate validation, HTTP/1.1 and HTTP/2 ALPN", tls)
    run_features(test, check, request, header, cookie_header, raw_exchange, port, tls_port, cert, target)


def wait_ready(work, name, process):
    end = time.monotonic() + 20
    while time.monotonic() < end:
        check(process.poll() is None, f"server exited; inspect {work / 'server.log'}")
        path = work / name
        if path.exists():
            try:
                return json.loads(path.read_text())["port"]
            except (ValueError, KeyError):
                pass
        time.sleep(0.02)
    raise AssertionError(f"server did not become ready; inspect {work / 'server.log'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hxwell", required=True, type=Path)
    parser.add_argument("--crossbyte", required=True, type=Path)
    parser.add_argument("--emitter", required=True, type=Path)
    parser.add_argument("--haxe", default="haxe")
    parser.add_argument("--target", choices=["jvm", "neko"], default="jvm")
    parser.add_argument("--concurrent", type=Path, help="Optional exact haxe-concurrent source directory")
    args = parser.parse_args()
    version = subprocess.check_output([args.haxe, "--version"], text=True).strip()
    check(version in ("4.3.7", "5.0.0-preview.1"), f"Unsupported Haxe compiler: {version}")
    check(version == "4.3.7" or args.target == "jvm", "Haxe 5 preview is verified on JVM only")
    (ROOT / "out").mkdir(exist_ok=True)
    report_path = ROOT / f"out/report-{args.target}.json"
    work = Path(tempfile.mkdtemp(prefix=f"integration-{args.target}-", dir=ROOT / "out"))
    for directory in ("resources/views", "public", "cache", "session"):
        (work / directory).mkdir(parents=True, exist_ok=True)
    (work / "public/hello.txt").write_text("static file works")
    (work / "public/.secret").write_text("PRIVATE")
    (work / "secret.txt").write_text("PRIVATE")
    (work / "public/escape.txt").symlink_to(work / "secret.txt")
    cert, key = work / "cert.pem", work / "key.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost",
                    "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1", "-keyout", str(key), "-out", str(cert)],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    hxwell, crossbyte, emitter = args.hxwell.resolve(), args.crossbyte.resolve(), args.emitter.resolve()
    # Test the real package/dependency path while keeping all dev registrations
    # local to this disposable build directory (never change global haxelib).
    local_libraries(work, ROOT, hxwell, crossbyte, emitter, args.concurrent, haxe5=version.startswith("5."))
    command = [args.haxe, "-lib", "hxwell-crossbyte", "-cp", str(hxwell / "tests/e2e/server/src"), "-cp", str(ROOT / "tests/src")]
    for define in ("disable_env", "disable_cli", "analyzer-optimize"):
        command += ["-D", define]
    if version.startswith("5."):
        command += ["-lib", "hx4compat"]
    # HxWell's extraParams.hxml supplies its main class.
    command += ["-lib", "hxjava", "--jvm", "server.jar"] if args.target == "jvm" else ["--neko", "server.n"]
    (work / "build-command.json").write_text(json.dumps(command, indent=2))
    report_path.write_text(json.dumps({"success": False, "phase": "compile", "haxe": version,
                                                     "workdir": str(work), "cases": []}, indent=2) + "\n")
    with (work / "build.log").open("w") as log:
        try:
            subprocess.run(command, cwd=work, stdout=log, stderr=subprocess.STDOUT, check=True)
        except subprocess.CalledProcessError:
            print("Build failed; inspect", work / "build.log", file=sys.stderr)
            raise
    env = dict(os.environ)
    if args.target == "jvm":
        env.update(CROSSBYTE_TEST_CERT=str(cert), CROSSBYTE_TEST_KEY=str(key))
    process = None
    success = False
    try:
        with (work / "server.log").open("w") as log:
            executable = ["java", "-Xmx128m", "-jar", "server.jar"] if args.target == "jvm" else ["neko", "server.n"]
            process = subprocess.Popen(executable, cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT)
            port = wait_ready(work, "ready.json", process)
            tls_port = wait_ready(work, "tls-ready.json", process) if args.target == "jvm" else None
            suite(port, tls_port, cert, args.target)
            (work / "ready.json").unlink()
            live_socket = WebSocket(port)
            check(request(port, "/restart")[2] == b"restarting")
            opcode, close_body = live_socket.receive()
            check(opcode == 8 and int.from_bytes(close_body[:2], "big") == 1001)
            live_socket.socket.sendall(frame(8, close_body))
            live_socket.close_transport()
            port = wait_ready(work, "ready.json", process)
            check(request(port, "/hello/restarted")[0] == 200)
            RESULTS.append({"name": "stop and restart driver", "passed": True})
            print("PASS stop and restart driver", flush=True)
            check(request(port, "/shutdown")[2] == b"stopping")
            check(process.wait(timeout=10) == 0, "server did not stop cleanly")
            RESULTS.append({"name": "graceful shutdown of both listeners", "passed": True})
            print("PASS graceful shutdown of both listeners", flush=True)
            success = True
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        report = {"success": success, "target": args.target, "haxe": version, "jvmHeap": "128 MiB" if args.target == "jvm" else None, "workdir": str(work), "cases": RESULTS,
                  "crossbyteRevision": revision(crossbyte), "hxwellRevision": revision(hxwell)}
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print("Logs:", work)
    print(f"PASS: {len(RESULTS)} integration groups")


if __name__ == "__main__":
    main()
