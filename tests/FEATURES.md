# Verified HxWell feature matrix

The integration runner starts `hx.well.HxWell` with real application config and
providers, then drives the Crossbyte transport through independent HTTP/1.1,
HTTP/2, TLS and WebSocket clients. The core runner executes the existing HxWell
unit/security suite with Crossbyte's standard-library overrides active.

| Feature family | Verification | Targets |
| --- | --- | --- |
| Config, provider boot, instance builders, multiple listeners | Normal HxWell entry point, main-thread root, secondary-listener startup, two I/O runtimes, stop/restart | JVM, Neko |
| Routing | Parameters, optional/regex routes, groups, names, redirects, domains, method restrictions | JVM, Neko |
| MethodHandler and validation | Annotated verbs, missing/valid body fields, 405 and 422 | JVM, Neko |
| Request data | Arbitrary headers, cookie maps, single query decode, JSON, forms, binary Input | JVM, Neko |
| Native request decoding | Fixed length, chunked, gzip, deflate and post-decode limits | JVM, Neko |
| Responses | String/JSON/nested IResponseInstance, explicit status, metadata, repeated cookies, files/Input, manual and deferred output | JVM, Neko |
| Bodyless/persistent responses | HEAD, 204/304, POST→GET and HEAD→GET pipelining; encoding isolation after reuse | JVM, Neko |
| Encoding options | Native gzip/deflate, explicit HxWell deflate levels and chunks, Empty options, q=0 | JVM, Neko |
| Templates | Concurrent independent rendering, core template isolation tests | JVM, Neko |
| Middleware/session/CSRF | Before/after hooks, storage, session regeneration, valid/invalid CSRF | JVM, Neko |
| Authentication | SessionAuth/RequireAuth, multiple guards, login/logout, replay rejection | JVM, Neko |
| SQL-backed authentication | Actual model lookup and BCrypt verification via Auth.attempt | Neko/SQLite |
| Cache, cryptography, commands and scheduling | Cache lifecycle, encryption/hash round trips, command arguments, scheduled task from a request | JVM, Neko |
| HTTP/2 | Multiplexed deferred responses and flow-controlled large output | JVM, Neko |
| TLS/WSS/ALPN | Trusted fixture certificate, HTTPS, secure WebSocket, HTTP/2 ALPN | JVM |
| WebSocket | Text/binary, broadcast, callbacks/error callbacks, query state, fragmentation, ping/pong, prefetched frame and invalid protocol/UTF-8 | JVM, Neko |
| WebSocket lifecycle | Middleware authorization, bounded queues, 140 handoffs past a 128-connection limit, active-session restart | JVM, Neko |
| Socket.IO-style APIs | Events, rooms, ping, direct connection, polling and upgrade | JVM, Neko |
| Concurrent Socket.IO state | 32 workers join/leave shared rooms; polling token can be claimed once | JVM core suite |
| Database and models | Real schema create/alter/index metadata, CRUD, transaction rollback, model serialization, ResultSetResponse, DB connections released after WS callbacks | Neko/SQLite |
| Schema/query language | Full HxWell grammar/query/security suite, including MySQL generation | JVM, Neko |
| Failure and security paths | Smuggling, original-header controls, duplicate framing, CRLF, hidden/symlink files, body/path/header limits, client disconnects and failed source Input | JVM, Neko |
| Backpressure and admission | 4 MiB response through 128 KiB native cap, responsive concurrent route, 32 deferred contexts and recovered slots | JVM, Neko |

Current boundaries: Haxe 4.3.7 with the pinned Crossbyte 1.0.1 Git snapshot;
JVM's native Haxe SQL implementations are stubs, so live SQL is tested on Neko.
There is no live MySQL service in this matrix, nor HTTP/2 extended-CONNECT
WebSocket, incremental request uploads, Node/browser/PHP execution, or a claim
of full Node.js Socket.IO protocol compatibility. These are not counted as
passing tests. The matrix covers HxWell's implemented feature families; it is
not a proof over every possible application or input.
