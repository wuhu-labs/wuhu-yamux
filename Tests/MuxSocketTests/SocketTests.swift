#if canImport(Glibc)
  import Glibc
#elseif canImport(Musl)
  import Musl
#elseif canImport(Darwin)
  import Darwin
#endif

import Mux
@testable import MuxSocket
import Testing

@Suite("TCP integration")
struct TCPIntegrationTests {
  @Test("TCP round-trip: open stream, send data, receive")
  func tcpRoundTrip() async throws {
    let listener = try await SocketListener.bind(host: "127.0.0.1", port: 0)
    guard let addr = listener.localAddress, let port = addr.port else {
      #expect(Bool(false), "No local address")
      return
    }

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

    let clientConn = try await SocketConnector.connect(host: "127.0.0.1", port: port)
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
    let listener = try await SocketListener.bind(host: "127.0.0.1", port: 0)
    let port = try #require(listener.localAddress?.port)

    let serverTask = Task { () -> [UInt8] in
      var connIter = listener.connections.makeAsyncIterator()
      let conn = await connIter.next()!
      let session = MuxSession(connection: conn, role: .responder, config: MuxConfig(keepaliveInterval: nil))
      let runTask = Task { try await session.run() }
      defer { runTask.cancel() }

      var streamIter = session.inbound.makeAsyncIterator()
      let stream = await streamIter.next()!

      var received: [UInt8] = []
      for await chunk in await stream.bytes {
        received.append(contentsOf: chunk)
      }

      try await stream.write([0xFF, 0xFE])
      try await stream.finish()
      await session.close()
      return received
    }

    let clientConn = try await SocketConnector.connect(host: "127.0.0.1", port: port)
    let session = MuxSession(connection: clientConn, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    let runTask = Task { try await session.run() }

    let stream = try await session.open()
    try await stream.write([1, 2, 3])
    try await stream.finish()

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
    let listener = try await SocketListener.bind(host: "127.0.0.1", port: 0)
    let port = try #require(listener.localAddress?.port)
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
          for await chunk in await s.bytes {
            d.append(contentsOf: chunk)
          }
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

    let clientConn = try await SocketConnector.connect(host: "127.0.0.1", port: port)
    let session = MuxSession(connection: clientConn, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    let runTask = Task { try await session.run() }

    var streams: [MuxStream] = []
    for _ in 0 ..< streamCount {
      try await streams.append(session.open())
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

// MARK: - Unix Domain Socket Integration

@Suite("Unix domain socket integration")
struct UDSIntegrationTests {
  /// Helper to generate a unique socket path for each test.
  private func uniqueSocketPath() -> String {
    "/tmp/wuhu-yamux-test-\(UInt32.random(in: 0 ... UInt32.max)).sock"
  }

  @Test("UDS round-trip: open stream, send data, receive")
  func udsRoundTrip() async throws {
    let path = uniqueSocketPath()

    let listener = try await SocketListener.bind(unixDomainSocketPath: path)

    let serverTask = Task { () -> [UInt8] in
      var connIter = listener.connections.makeAsyncIterator()
      let conn = await connIter.next()!
      let session = MuxSession(connection: conn, role: .responder, config: MuxConfig(keepaliveInterval: nil))
      let runTask = Task { try await session.run() }
      defer { runTask.cancel() }

      var streamIter = session.inbound.makeAsyncIterator()
      let stream = await streamIter.next()!
      var data: [UInt8] = []
      for await chunk in await stream.bytes {
        data.append(contentsOf: chunk)
      }
      try await stream.finish()
      await session.close()
      return data
    }

    let clientConn = try await SocketConnector.connect(unixDomainSocketPath: path)
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

  @Test("UDS bidirectional streams")
  func udsBidirectional() async throws {
    let path = uniqueSocketPath()

    let listener = try await SocketListener.bind(unixDomainSocketPath: path)

    let serverTask = Task { () -> [UInt8] in
      var connIter = listener.connections.makeAsyncIterator()
      let conn = await connIter.next()!
      let session = MuxSession(connection: conn, role: .responder, config: MuxConfig(keepaliveInterval: nil))
      let runTask = Task { try await session.run() }
      defer { runTask.cancel() }

      var streamIter = session.inbound.makeAsyncIterator()
      let stream = await streamIter.next()!

      var received: [UInt8] = []
      for await chunk in await stream.bytes {
        received.append(contentsOf: chunk)
      }

      try await stream.write([0xAA, 0xBB])
      try await stream.finish()
      await session.close()
      return received
    }

    let clientConn = try await SocketConnector.connect(unixDomainSocketPath: path)
    let session = MuxSession(connection: clientConn, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    let runTask = Task { try await session.run() }

    let stream = try await session.open()
    try await stream.write([10, 20, 30])
    try await stream.finish()

    var response: [UInt8] = []
    for await chunk in await stream.bytes {
      response.append(contentsOf: chunk)
    }

    #expect(response == [0xAA, 0xBB])
    #expect(try await serverTask.value == [10, 20, 30])

    await session.close()
    await listener.close()
    runTask.cancel()
  }

  @Test("Multiple concurrent streams over UDS")
  func udsMultipleStreams() async throws {
    let path = uniqueSocketPath()
    let streamCount = 5

    let listener = try await SocketListener.bind(unixDomainSocketPath: path)

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
          for await chunk in await s.bytes {
            d.append(contentsOf: chunk)
          }
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

    let clientConn = try await SocketConnector.connect(unixDomainSocketPath: path)
    let session = MuxSession(connection: clientConn, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    let runTask = Task { try await session.run() }

    var streams: [MuxStream] = []
    for _ in 0 ..< streamCount {
      try await streams.append(session.open())
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

  @Test("UDS stale socket cleanup: removes stale socket file on bind")
  func udsStaleSocketCleanup() async throws {
    let path = uniqueSocketPath()

    // Create a stale socket file using raw POSIX syscalls.
    // This simulates a server that crashed without cleaning up.
    let fd = socket(AF_UNIX, Int32(SOCK_STREAM.rawValue), 0)
    #expect(fd >= 0, "Failed to create socket")

    var addr = sockaddr_un()
    addr.sun_family = sa_family_t(AF_UNIX)
    let pathBytes = path.utf8CString
    let sunPathSize = MemoryLayout.size(ofValue: addr.sun_path)
    precondition(pathBytes.count <= sunPathSize, "Socket path too long")
    withUnsafeMutableBytes(of: &addr.sun_path) { buf in
      for (i, byte) in pathBytes.enumerated() where i < sunPathSize {
        buf[i] = UInt8(bitPattern: byte)
      }
    }

    let bindResult = withUnsafePointer(to: &addr) { ptr in
      ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockPtr in
        bind(fd, sockPtr, socklen_t(MemoryLayout<sockaddr_un>.size))
      }
    }
    #expect(bindResult == 0, "Failed to bind socket")
    close(fd)

    // Verify the stale socket file exists
    var st = stat()
    #expect(lstat(path, &st) == 0, "Stale socket file should exist")
    #expect((st.st_mode & S_IFMT) == S_IFSOCK, "Should be a socket file")

    // Binding again with cleanup should succeed
    let listener = try await SocketListener.bind(unixDomainSocketPath: path)

    // Verify we can actually use it
    let serverTask = Task { () -> [UInt8] in
      var connIter = listener.connections.makeAsyncIterator()
      let conn = await connIter.next()!
      let session = MuxSession(connection: conn, role: .responder, config: MuxConfig(keepaliveInterval: nil))
      let runTask = Task { try await session.run() }
      defer { runTask.cancel() }

      var streamIter = session.inbound.makeAsyncIterator()
      let stream = await streamIter.next()!
      var data: [UInt8] = []
      for await chunk in await stream.bytes {
        data.append(contentsOf: chunk)
      }
      try await stream.finish()
      await session.close()
      return data
    }

    let clientConn = try await SocketConnector.connect(unixDomainSocketPath: path)
    let session = MuxSession(connection: clientConn, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    let runTask = Task { try await session.run() }

    let stream = try await session.open()
    try await stream.write([1, 2, 3])
    try await stream.finish()

    #expect(try await serverTask.value == [1, 2, 3])

    await session.close()
    await listener.close()
    runTask.cancel()
  }

  @Test("UDS bind fails if socket is live")
  func udsLiveSocketDetection() async throws {
    let path = uniqueSocketPath()

    // Create a live listener
    let listener = try await SocketListener.bind(unixDomainSocketPath: path)

    // Trying to bind again should fail with socketAlreadyInUse
    do {
      _ = try await SocketListener.bind(unixDomainSocketPath: path)
      #expect(Bool(false), "Should have thrown socketAlreadyInUse")
    } catch let error as SocketError {
      guard case .socketAlreadyInUse = error else {
        #expect(Bool(false), "Expected socketAlreadyInUse, got \(error)")
        return
      }
    }

    await listener.close()
  }

  @Test("UDS close removes socket file")
  func udsCloseRemovesSocket() async throws {
    let path = uniqueSocketPath()

    let listener = try await SocketListener.bind(unixDomainSocketPath: path)

    // Socket file should exist
    var stat = stat()
    #expect(lstat(path, &stat) == 0, "Socket file should exist while listening")

    await listener.close()

    // Socket file should be removed after close
    #expect(lstat(path, &stat) != 0, "Socket file should be removed after close")
  }

  @Test("UDS bind fails on non-socket file at path")
  func udsNonSocketFile() async throws {
    let path = uniqueSocketPath()

    // Create a regular file at the path
    let fd = open(path, O_CREAT | O_WRONLY, 0o644)
    close(fd)
    defer { unlink(path) }

    do {
      _ = try await SocketListener.bind(unixDomainSocketPath: path)
      #expect(Bool(false), "Should have thrown pathExistsButNotSocket")
    } catch let error as SocketError {
      guard case .pathExistsButNotSocket = error else {
        #expect(Bool(false), "Expected pathExistsButNotSocket, got \(error)")
        return
      }
    }
  }
}
