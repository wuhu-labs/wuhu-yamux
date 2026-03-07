import Testing
import Mux
@testable import MuxWebSocket
import Hummingbird
import HummingbirdCore
import HummingbirdWebSocket
import WSClient
import NIOCore
import NIOPosix
import Logging
import HTTPTypes

@Suite("WebSocket integration")
struct WebSocketIntegrationTests {
  @Test("Small payload round-trip")
  func wsRoundTrip() async throws {
    let result = try await sendAndReceive(payload: [10, 20, 30, 40])
    #expect(result == [10, 20, 30, 40])
  }

  @Test("Large payload (128KB) round-trips over WebSocket")
  func largePayloadRoundTrip() async throws {
    let payloadSize = 128 * 1024
    let payload = (0 ..< payloadSize).map { UInt8(truncatingIfNeeded: $0) }
    let result = try await sendAndReceive(payload: payload)
    #expect(result.count == payloadSize)
    #expect(result == payload)
  }

  @Test(
    "Boundary: payload at and around the 16KB WebSocket frame limit",
    arguments: [
      // Exactly one full frame — length == maxFrameSize, decoder check is
      // `length > maxFrameSize` so this must pass.
      (1 << 14),
      // One byte over — requires chunking into two frames.
      (1 << 14) + 1,
      // Two full frames exactly.
      (1 << 14) * 2,
      // Two frames + 1 byte remainder.
      (1 << 14) * 2 + 1,
    ]
  )
  func frameSizeBoundary(payloadSize: Int) async throws {
    let payload = (0 ..< payloadSize).map { UInt8(truncatingIfNeeded: $0) }
    let result = try await sendAndReceive(payload: payload)
    #expect(result.count == payloadSize)
    #expect(result == payload)
  }

  @Test("Bidirectional large payload: server echoes back >16KB data")
  func bidirectionalLargePayload() async throws {
    let port = Int.random(in: 20000 ..< 30000)
    let clientResult = _SharedData()

    let payloadSize = 64 * 1024
    let payload = (0 ..< payloadSize).map { UInt8(truncatingIfNeeded: $0) }

    let router = Router()
    let app = Application(
      router: router,
      server: .http1WebSocketUpgrade { _, _, _ in
        .upgrade([:]) { inbound, outbound, _ in
          let conn = WebSocketConnection(inbound: inbound, outbound: outbound)
          let session = MuxSession(
            connection: conn, role: .responder,
            config: MuxConfig(keepaliveInterval: nil)
          )
          try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await session.run() }
            group.addTask {
              var streamIter = session.inbound.makeAsyncIterator()
              if let stream = await streamIter.next() {
                var data: [UInt8] = []
                for await chunk in await stream.bytes {
                  data.append(contentsOf: chunk)
                }
                try await stream.write(data)
                try await stream.finish()
              }
              await session.close()
            }
            try await group.next()
            group.cancelAll()
          }
        }
      },
      configuration: .init(address: .hostname("127.0.0.1", port: port))
    )

    let serverTask = Task { try await app.run() }
    try await Task.sleep(for: .milliseconds(500))

    try await WebSocketClient.connect(
      url: .init("ws://127.0.0.1:\(port)"),
      logger: Logger(label: "test")
    ) { inbound, outbound, _ in
      let conn = WebSocketConnection(inbound: inbound, outbound: outbound)
      let session = MuxSession(
        connection: conn, role: .initiator,
        config: MuxConfig(keepaliveInterval: nil)
      )
      let runTask = Task { try await session.run() }
      defer { runTask.cancel() }

      let stream = try await session.open()
      try await stream.write(payload)
      try await stream.finish()

      var echoed: [UInt8] = []
      for await chunk in await stream.bytes {
        echoed.append(contentsOf: chunk)
      }
      await clientResult.set(echoed)

      try await Task.sleep(for: .milliseconds(200))
      await session.close()
    }

    let result = await clientResult.get()
    #expect(result.count == payloadSize)
    #expect(result == payload)

    serverTask.cancel()
  }
}

// MARK: - Helpers

/// Spins up a WebSocket server + client, sends `payload` from client to server
/// on a mux stream, and returns what the server received.
private func sendAndReceive(payload: [UInt8]) async throws -> [UInt8] {
  let port = Int.random(in: 20000 ..< 30000)
  let serverData = _SharedData()

  let router = Router()
  let app = Application(
    router: router,
    server: .http1WebSocketUpgrade { _, _, _ in
      .upgrade([:]) { inbound, outbound, _ in
        let conn = WebSocketConnection(inbound: inbound, outbound: outbound)
        let session = MuxSession(
          connection: conn, role: .responder,
          config: MuxConfig(keepaliveInterval: nil)
        )
        try await withThrowingTaskGroup(of: Void.self) { group in
          group.addTask { try await session.run() }
          group.addTask {
            var streamIter = session.inbound.makeAsyncIterator()
            if let stream = await streamIter.next() {
              var data: [UInt8] = []
              for await chunk in await stream.bytes {
                data.append(contentsOf: chunk)
              }
              await serverData.set(data)
              try await stream.finish()
            }
            await session.close()
          }
          try await group.next()
          group.cancelAll()
        }
      }
    },
    configuration: .init(address: .hostname("127.0.0.1", port: port))
  )

  let serverTask = Task { try await app.run() }
  try await Task.sleep(for: .milliseconds(500))

  try await WebSocketClient.connect(
    url: .init("ws://127.0.0.1:\(port)"),
    logger: Logger(label: "test")
  ) { inbound, outbound, _ in
    let conn = WebSocketConnection(inbound: inbound, outbound: outbound)
    let session = MuxSession(
      connection: conn, role: .initiator,
      config: MuxConfig(keepaliveInterval: nil)
    )
    let runTask = Task { try await session.run() }
    defer { runTask.cancel() }

    let stream = try await session.open()
    try await stream.write(payload)
    try await stream.finish()

    try await Task.sleep(for: .milliseconds(500))
    await session.close()
  }

  let result = await serverData.get()
  serverTask.cancel()
  return result
}

private actor _SharedData {
  var data: [UInt8] = []
  func set(_ d: [UInt8]) { data = d }
  func get() -> [UInt8] { data }
}
