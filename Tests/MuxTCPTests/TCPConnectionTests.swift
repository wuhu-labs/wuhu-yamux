import Testing
import Mux
@testable import MuxTCP

@Suite("TCP integration")
struct TCPIntegrationTests {
  @Test("TCP round-trip: open stream, send data, receive")
  func tcpRoundTrip() async throws {
    let listener = try await TCPListener.bind(host: "127.0.0.1", port: 0)
    guard let addr = listener.localAddress, let port = addr.port else {
      #expect(Bool(false), "No local address")
      return
    }

    // Server task: accept one connection, accept one stream, read data
    let serverTask = Task { () -> [UInt8] in
      var connIter = listener.connections.makeAsyncIterator()
      guard let conn = await connIter.next() else { return [] }
      let session = MuxSession(connection: conn, role: .responder, config: MuxConfig(keepaliveInterval: nil))
      let runTask = Task { try await session.run() }
      defer { runTask.cancel() }

      var streamIter = session.inbound.makeAsyncIterator()
      guard let stream = await streamIter.next() else { return [] }
      var data: [UInt8] = []
      for await chunk in await stream.bytes {
        data.append(contentsOf: chunk)
      }
      try await stream.finish()
      await session.close()
      return data
    }

    // Client
    let clientConn = try await TCPConnector.connect(host: "127.0.0.1", port: port)
    let clientSession = MuxSession(connection: clientConn, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    let clientRun = Task { try await clientSession.run() }

    let stream = try await clientSession.open()
    let payload: [UInt8] = Array(0 ..< 200)
    try await stream.write(payload)
    try await stream.finish()

    let result = try await serverTask.value
    #expect(result == payload)

    await clientSession.close()
    await listener.close()
    clientRun.cancel()
  }

  @Test("TCP bidirectional streams")
  func tcpBidirectional() async throws {
    let listener = try await TCPListener.bind(host: "127.0.0.1", port: 0)
    let port = listener.localAddress!.port!

    let serverTask = Task { () -> [UInt8] in
      var connIter = listener.connections.makeAsyncIterator()
      let conn = await connIter.next()!
      let session = MuxSession(connection: conn, role: .responder, config: MuxConfig(keepaliveInterval: nil))
      let runTask = Task { try await session.run() }
      defer { runTask.cancel() }

      var streamIter = session.inbound.makeAsyncIterator()
      let stream = await streamIter.next()!

      // Read what client sent
      var received: [UInt8] = []
      for await chunk in await stream.bytes {
        received.append(contentsOf: chunk)
      }

      // Send response back
      try await stream.write([0xFF, 0xFE])
      try await stream.finish()
      await session.close()
      return received
    }

    let clientConn = try await TCPConnector.connect(host: "127.0.0.1", port: port)
    let session = MuxSession(connection: clientConn, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    let runTask = Task { try await session.run() }

    let stream = try await session.open()
    try await stream.write([1, 2, 3])
    try await stream.finish()

    // Read server's response
    var response: [UInt8] = []
    for await chunk in await stream.bytes {
      response.append(contentsOf: chunk)
    }

    #expect(response == [0xFF, 0xFE])
    #expect(try await serverTask.value == [1, 2, 3])

    await session.close()
    await listener.close()
    runTask.cancel()
  }

  @Test("Multiple concurrent streams over TCP")
  func tcpMultipleStreams() async throws {
    let listener = try await TCPListener.bind(host: "127.0.0.1", port: 0)
    let port = listener.localAddress!.port!
    let streamCount = 5

    let serverTask = Task { () -> [UInt32: [UInt8]] in
      var connIter = listener.connections.makeAsyncIterator()
      let conn = await connIter.next()!
      let session = MuxSession(connection: conn, role: .responder, config: MuxConfig(keepaliveInterval: nil))
      let runTask = Task { try await session.run() }
      defer { runTask.cancel() }

      var results: [UInt32: [UInt8]] = [:]
      var tasks: [Task<(UInt32, [UInt8]), Never>] = []
      var count = 0
      for await s in session.inbound {
        let t = Task { () -> (UInt32, [UInt8]) in
          var d: [UInt8] = []
          for await chunk in await s.bytes { d.append(contentsOf: chunk) }
          return (s.id, d)
        }
        tasks.append(t)
        count += 1
        if count >= streamCount { break }
      }
      for t in tasks {
        let (id, data) = await t.value
        results[id] = data
      }
      await session.close()
      return results
    }

    let clientConn = try await TCPConnector.connect(host: "127.0.0.1", port: port)
    let session = MuxSession(connection: clientConn, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    let runTask = Task { try await session.run() }

    var streams: [MuxStream] = []
    for _ in 0 ..< streamCount {
      streams.append(try await session.open())
    }

    try await withThrowingTaskGroup(of: Void.self) { group in
      for (i, s) in streams.enumerated() {
        group.addTask {
          try await s.write([UInt8](repeating: UInt8(i), count: 100))
          try await s.finish()
        }
      }
      try await group.waitForAll()
    }

    let results = try await serverTask.value
    for (i, s) in streams.enumerated() {
      #expect(results[s.id] == [UInt8](repeating: UInt8(i), count: 100))
    }

    await session.close()
    await listener.close()
    runTask.cancel()
  }
}

@Suite("Unix domain socket integration")
struct UDSIntegrationTests {
  @Test("UDS round-trip")
  func udsRoundTrip() async throws {
    let path = "/tmp/wuhu-yamux-test-\(UInt32.random(in: 0 ... UInt32.max)).sock"
    defer { unlink(path) }

    let listener = try await TCPListener.bind(unixDomainSocketPath: path)

    let serverTask = Task { () -> [UInt8] in
      var connIter = listener.connections.makeAsyncIterator()
      let conn = await connIter.next()!
      let session = MuxSession(connection: conn, role: .responder, config: MuxConfig(keepaliveInterval: nil))
      let runTask = Task { try await session.run() }
      defer { runTask.cancel() }

      var streamIter = session.inbound.makeAsyncIterator()
      let stream = await streamIter.next()!
      var data: [UInt8] = []
      for await chunk in await stream.bytes { data.append(contentsOf: chunk) }
      try await stream.finish()
      await session.close()
      return data
    }

    let clientConn = try await TCPConnector.connect(unixDomainSocketPath: path)
    let session = MuxSession(connection: clientConn, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    let runTask = Task { try await session.run() }

    let stream = try await session.open()
    try await stream.write([42, 43, 44])
    try await stream.finish()

    #expect(try await serverTask.value == [42, 43, 44])

    await session.close()
    await listener.close()
    runTask.cancel()
  }
}

#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif
