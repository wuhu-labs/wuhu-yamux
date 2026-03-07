# wuhu-yamux

Symmetric multiplexed stream protocol for Swift, inspired by
[yamux](https://github.com/hashicorp/yamux/blob/master/spec.md).

Provides independent, concurrent, flow-controlled streams over any reliable
ordered byte stream (TCP, WebSocket, Unix domain socket, in-memory pipe).

## Targets

| Target | Description | Dependencies |
|--------|-------------|--------------|
| `Mux` | Core yamux protocol — frame codec, session, streams, flow control, keepalive | None (pure Swift concurrency) |
| `MuxTCP` | TCP and Unix domain socket transports | SwiftNIO |
| `MuxWebSocket` | WebSocket transport | [swift-websocket](https://github.com/hummingbird-project/swift-websocket) (`WSCore`) |

## Usage

```swift
// Add to Package.swift
.package(url: "https://github.com/wuhu-labs/wuhu-yamux.git", from: "0.1.0")

// Import the target you need
import Mux       // Core protocol
import MuxTCP    // TCP transport
import MuxWebSocket // WebSocket transport
```

## Requirements

- Swift 6.2+
- macOS 14+ / iOS 16+ / Linux

## License

MIT
