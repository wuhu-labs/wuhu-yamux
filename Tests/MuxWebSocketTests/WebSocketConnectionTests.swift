import Testing
import Mux
@testable import MuxWebSocket
import Hummingbird
import HummingbirdCore
import HummingbirdWebSocket
import HummingbirdWSClient
import NIOCore
import NIOPosix
import Logging
import HTTPTypes

@Suite("WebSocket integration")
struct WebSocketIntegrationTests {
  @Test("WebSocket round-trip: open stream, send data, receive")
  func wsRoundTrip() async throws {
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

    // Client
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
      try await stream.write([10, 20, 30, 40])
      try await stream.finish()

      try await Task.sleep(for: .milliseconds(500))
      await session.close()
    }

    let result = await serverData.get()
    #expect(result == [10, 20, 30, 40])

    serverTask.cancel()
  }
}

private actor _SharedData {
  var data: [UInt8] = []
  func set(_ d: [UInt8]) { data = d }
  func get() -> [UInt8] { data }
}
