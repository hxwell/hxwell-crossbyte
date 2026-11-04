# HxWell Crossbyte driver

An optional [HxWell](https://github.com/hxwell/hxwell) HTTP and WebSocket transport built on
[Crossbyte](https://github.com/DimensionscapeOrg/crossbyte). It implements HxWell's
`AbstractHttpDriver`, `IDriverContext` and instance builder interfaces.

Crossbyte owns the network event loop and HTTP framing. HxWell keeps routing,
middleware, sessions, CSRF, public files and response generation. On JVM every
HxWell request runs on a Java virtual thread; socket operations return to the
connection's owning Crossbyte runtime through its `post` API.

## Requirements and installation

- **Haxe 4.3.7** or **5.0.0-preview.1** with **Java 21+**. Haxe 5 requires
  `-lib hx4compat` (1.0.0). Neko remains supported with Haxe 4.3.7. CI covers
  both JVM compilers on Java 21/25 and Haxe 4/Neko/SQLite.
- HxWell [`7944b82`](https://github.com/hxwell/hxwell/commit/7944b82b589646baf67dba5f2a1954b9f040b30e)
  or newer, including the JSON status, const guard, command argument and
  Socket.IO concurrency fixes verified by this suite.
- Crossbyte's **1.0.1 Git snapshot**, tested at
  [`45628a59667de98649c23cbcd391513a1ef714c4`](https://github.com/DimensionscapeOrg/crossbyte/commit/45628a59667de98649c23cbcd391513a1ef714c4).
- Emitter 2.0.0 and HxWell's normal Haxelib dependencies.

The published Crossbyte version was 1.0.0 when this adapter was validated. Install
the tested Git snapshot; the adapter checks its version at compile time.
Haxe 5 compatibility is provided by aliases and a map-size extension under `compat/haxe5`.
They bridge the moved Java primitive types, the native class helpers and the
legacy HTTP/2 secondary-type reference. A `HashMap.size()` extension keeps the
unmodified haxe-concurrent 5.1.5 package usable with the preview standard library.
The Crossbyte runtime source stays
unchanged. The aliases are added only for Haxe 5.0.0-preview.1 on JVM; the
Haxe 4 classpath and native targets are unchanged. No extra virtual thread is
introduced: JVM requests already enter `HttpHandler.process()` through HxWell's
`jvm.thread.VirtualThread.create(work).start()` helper.

```sh
haxelib install emitter 2.0.0
haxelib install hxjava
haxelib git crossbyte https://github.com/DimensionscapeOrg/crossbyte.git 45628a59667de98649c23cbcd391513a1ef714c4
haxelib dev hxwell /path/to/hxwell
haxelib dev hxwell-crossbyte /path/to/hxwell-crossbyte
```

Add `-lib hxwell-crossbyte` to the HxWell application's build. The updated HxWell
project template selects `CrossbyteInstance` when that library is enabled. For an
existing application, use [examples/InstanceConfig.hx](examples/InstanceConfig.hx)
as `src/hx/well/config/InstanceConfig.hx`:

```haxe
import hx.well.http.driver.crossbyte.CrossbyteInstance;

// Inside InstanceConfig.get():
return [CrossbyteInstance.builder()
    .setHost("0.0.0.0")
    .setPort(3000)
    .setMaxConnections(1000)
    .setMaxConcurrentRequests(256)
    .configureHttp(http -> http.http2Enabled = true)
    .build()];
```

Build from the application folder using Haxe 4.3.7 (or add `-lib hx4compat` with Haxe 5.0.0-preview.1):

```sh
haxe -cp src -lib hxwell-crossbyte -lib hxjava --jvm out/server.jar
java -jar out/server.jar start
```

HxWell supplies its `main` through its Haxelib build arguments. Initialize the
builder on the main thread, as `InstanceConfig.get()` does. It establishes a
hosted Crossbyte root without taking over HxWell's CLI lifetime. Each listener
runs on its own child poll loop; additional HxWell instances can start on their
usual background threads. `onStart` fires after binding succeeds. Port `0` is
supported; a driver's `localPort` exposes the selected port.

## HTTP behavior

Supported and exercised on real connections:

- HTTP/1.1 persistence and pipelining; HTTP/2 multiplexing and flow control.
- TLS using PEM certificates/keys, including ALPN for HTTP/1.1 and HTTP/2.
- Routing, method errors, headers, JSON/form bodies and multiple `Set-Cookie`
  fields; HxWell session and CSRF middleware; secure public-file handling.
- Fixed-length, chunked, gzip and deflate request bodies.
- RFC6455 WebSocket/WSS upgrades, binary/text, fragmentation, ping/pong,
  broadcast, callbacks, middleware authorization and bounded message queues.
- HxWell's Socket.IO-style events, rooms, heartbeat and polling upgrade.
- `InputResponse`, binary bodies, manual `beginWrite` output and `AsyncResponse`.
- HEAD and bodiless statuses, request-local response metadata, disconnect
  cleanup, bounded admission, graceful shutdown and driver restart.

For TLS, configure `.setTls("/path/cert.pem", "/path/key.pem")`. `setSsl(true)`
without both paths is refused. `configureHttp` exposes Crossbyte's native server
configuration, including compression, HTTP/2, timeouts and rate limits. Crossbyte
owns automatic response compression when `encodingOptions` is null. HxWell's
`DeflateEncodingOptions` use a streaming zlib encoder with the selected level and
chunk size; `EmptyEncodingOptions` disables compression. Explicit options are
isolated per response and do not change the shared native compression policy.
The native default rate limiter remains active unless explicitly
configured by the application.

The adapter owns the native middleware callback and disables Crossbyte's static
file and PHP fallbacks so all application routing goes through HxWell.

### Body and output limits

Crossbyte buffers and decodes an entire request body before invoking middleware.
HxWell's configured `http.max_content_length` is applied to that native reader
and checked again by the adapter. `setStream(true)` routes receive a bounded
`BytesInput` over the decoded body; they do not receive an incremental upload.
JSON/form parsing uses these decoded bytes, without reinterpreting wire
`Content-Length` or decoding chunk framing a second time.

`http.max_buffer` and `http.max_path_length` are checked before routing. HTTP/1
headers are also checked in their original form to reject control characters
that Crossbyte otherwise trims, duplicate framing/Host fields, bare LF and extra
request-line fields. Rejected requests close the connection. Chunked requests
remain supported.

Responses stream in slices of at most 32 KiB. Only one slice per producer waits
at a time; the writer waits for output capacity **before** submitting a slice,
and for native drain notifications after backpressure. It never replays an
accepted slice. A positive native `maxOutputBufferSize` must be at least 64 KiB.
The integration test sends a 4 MiB body through a 128 KiB output cap to a slow
reader while another request remains responsive.

Streaming responses use native chunked framing for HTTP/1.1 and DATA frames for
HTTP/2. Crossbyte chooses response framing; application `Content-Length` and
`Transfer-Encoding` headers are not forwarded. Native response streams also
suppress bodies for HEAD, 204 and 304.

`maxConcurrentRequests` includes deferred responses and is released on completion
or client disconnect. Overflow receives 503. `setWriteTimeout(seconds)` bounds
blocked writes, and `setShutdownTimeout(seconds)` bounds graceful drain.

### Deferred and manual responses

Return `ResponseBuilder.asAsync()` to keep a context open. The adapter captures
that request's static response metadata before HxWell clears its thread locals.
A worker can later call `request.context.writeResponse(response)`; it closes the
response automatically. Manual output uses `beginWrite()`, `write*()` or `output`,
then `close()` to terminate the response. Return `asAsync()` after manual output
so HxWell does not try to send another response.

Response production must run on a worker, not on the Crossbyte I/O runtime.
Blocking an I/O runtime waiting for its own output is explicitly rejected.

### WebSocket lifecycle

The HTTP middleware chain authorizes the upgrade before the connection is handed
to Crossbyte's RFC6455 framer. Request headers and cookies survive the handshake.
Buffered bytes following the upgrade head are preserved, including a first frame
sent in the same packet. HxWell callbacks run on a serial worker per connection
(a virtual thread on JVM), and outgoing messages use the HxWell session queue.
Database connections and request/response thread locals are released after each
callback. The native HTTP connection slot is released when the WebSocket closes.

Inbound and outbound application queues are bounded by
`maxWebSocketQueuedBytes` and `maxWebSocketQueuedMessages`; oversize messages are
bounded by `maxWebSocketMessageSize`. Server shutdown closes sessions with 1001.
Tests exercise connection churn beyond the HTTP connection limit, slow readers,
protocol errors, authorized upgrades, broadcast and restart with an open session.

### Current boundaries

- JVM and Neko are verified targets. Other threaded sys targets retain a worker
  pool implementation but are not claimed as verified. Node.js, browser and PHP
  targets are rejected at compile time by this adapter.
- JVM's Haxe `sys.db` drivers are stubs. Live SQL, ORM, schema, result-set output
  and `Auth.attempt` are verified through this transport on Neko/SQLite. MySQL
  grammar generation is covered by HxWell's core suite; a live MySQL server is
  not part of this matrix.
- WebSocket uses HTTP/1.1 Upgrade, including WSS. HTTP/2 extended CONNECT is not
  implemented. Socket.IO tests cover the APIs HxWell currently implements, not
  every feature of the separate Node.js Socket.IO implementation.
- A small compatibility shim accesses native headers, protocol, original HTTP/1
  header bytes, output capacity, abort/drain state and WebSocket ownership that
  Crossbyte 1.0.1 does not expose publicly. These accesses are isolated in
  `internal/UpstreamAccess` and `internal/UpstreamWebSocket` and covered by wire
  tests; review them when updating Crossbyte.

## Validation

```sh
python3 -m venv .venv
.venv/bin/pip install -r tests/requirements.txt
.venv/bin/python tests/run.py \
    --hxwell /path/to/hxwell \
    --crossbyte /path/to/crossbyte \
    --emitter /path/to/emitter \
    --haxe /path/to/haxe-4.3.7/haxe
```

Use `--target neko` for the SQLite/ORM run. Run `tests/core.py` with the same
arguments and target to execute HxWell's complete unit/security suite with
Crossbyte's classpaths and standard-library overrides enabled.

The test runner registers dependencies in a fresh **local** Haxelib repository,
builds with `-lib hxwell-crossbyte`, starts the real `hx.well.HxWell` entry point
with an `InstanceConfig`, and uses independent Python HTTP/1.1, HTTP/2 and TLS
clients. It creates temporary self-signed test certificates with OpenSSL, trusts
only that certificate, and uses a 128 MiB JVM heap. Application fixtures, session
state, logs and generated binaries stay under `out/`; global Haxelib registrations
are unchanged.

`out/report-jvm.json`, `out/report-neko.json` and `out/core-*.json` record success
or failure, test groups/counts, dependency revisions and log locations. See
[tests/FEATURES.md](tests/FEATURES.md) for the feature-to-test mapping and boundaries.

[GitHub Actions](.github/workflows/test.yml) runs the integration and core suites
on push, pull request and manual dispatch. Actions and Crossbyte/Emitter sources
are pinned; HxWell is checked out from its current `master` and its resolved
revision is recorded. CI retains reports and logs, excluding test certificates.
