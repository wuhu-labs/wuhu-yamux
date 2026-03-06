# AGENTS.md

## What is wuhu-yamux

A symmetric multiplexed stream protocol for Swift, implementing the
[yamux specification](https://github.com/hashicorp/yamux/blob/master/spec.md).
Provides independent, concurrent, flow-controlled streams over any reliable
ordered byte stream.

## Build & Test

```bash
swift build          # Build all targets
swift test           # Run all tests
swift build -c release  # Release build
```

## Architecture

Three library targets with strict dependency layering:

### `Mux` — Core protocol (zero external dependencies)

Pure Swift concurrency implementation of the yamux wire protocol.

Key types:
- **`Connection`** protocol — byte stream abstraction. `read`, `write`, `close`.
- **`InMemoryConnection`** — paired in-memory pipe for testing. Created via
  `InMemoryConnection.makePair()`.
- **`MuxSession`** — the multiplexer. Wraps a `Connection`, manages stream table,
  flow control, keepalive, GoAway. Call `run()` in a long-lived task, use `open()`
  to create outbound streams, iterate `inbound` for inbound streams.
- **`MuxStream`** — a single multiplexed stream. Read via `bytes` AsyncSequence,
  write via `write(_:)`, half-close via `finish()`, abort via `reset()`.
- **`Frame`** — 12-byte yamux header + payload. Types: Data, WindowUpdate, Ping, GoAway.
- **`MuxConfig`** — tuning: window size, keepalive interval/timeout, max frame payload.
- **`MuxSessionRole`** — `.initiator` (odd stream IDs) or `.responder` (even stream IDs).

### `MuxTCP` — TCP / Unix domain socket transport (SwiftNIO)

- **`TCPConnection`** — wraps a NIO `Channel` as a `Connection`.
- **`TCPListener`** — accepts inbound connections, yields `MuxSession`s.
- **`TCPConnector`** — dials a remote address, returns a `MuxSession`.
- Unix domain socket support via the same types with UDS addresses.

### `MuxWebSocket` — WebSocket transport (Hummingbird)

- **`WebSocketConnection`** — wraps a Hummingbird WebSocket as a `Connection`.
- Server-side upgrade handler helpers.
- Client-side connector helpers.

## Conventions

- Swift 6.2, `-strict-concurrency=complete`
- 2-space indentation (see `.swiftformat`)
- No locks, no `DispatchQueue`, no `os_unfair_lock` — actors and Swift concurrency only
- All public types must be `Sendable`
- Tests use Swift Testing (`import Testing`, `@Test`, `#expect`)
- `Mux` target has zero external dependencies — pure Swift + concurrency runtime

## yamux Wire Format

```
Frame header (12 bytes, big-endian):
┌─────────┬──────┬───────┬──────────┬──────────┐
│ Version │ Type │ Flags │ StreamID │ Length   │
│ 1 byte  │1 byte│2 bytes│ 4 bytes  │ 4 bytes  │
└─────────┴──────┴───────┴──────────┴──────────┘
```

- Version: always 0
- Type: 0x0 Data, 0x1 Window Update, 0x2 Ping, 0x3 GoAway
- Flags: SYN (0x1), ACK (0x2), FIN (0x4), RST (0x8)
- StreamID: 0 for session frames. Odd = initiator, even = responder.
- Length: payload bytes (Data), window delta (WindowUpdate), opaque (Ping), error code (GoAway)
- Default initial stream window: 256 KB
