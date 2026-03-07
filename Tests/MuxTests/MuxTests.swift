@testable import Mux
import Testing

// MARK: - Helper

private func readBytes(
  from connection: any Connection,
  maxCount: Int,
) async throws -> [UInt8] {
  let buf = UnsafeMutableRawBufferPointer.allocate(byteCount: maxCount, alignment: 1)
  defer { buf.deallocate() }
  let n = try await connection.read(into: buf)
  return Array(UnsafeRawBufferPointer(rebasing: buf.prefix(n)))
}

/// Helper: create a session pair with keepalive disabled, run both, execute body, then clean up.
private func withSessionPair(
  config: MuxConfig = MuxConfig(keepaliveInterval: nil),
  body: (MuxSession, MuxSession) async throws -> Void,
) async throws {
  let (connA, connB) = InMemoryConnection.makePair()
  let sessionA = MuxSession(connection: connA, role: .initiator, config: config)
  let sessionB = MuxSession(connection: connB, role: .responder, config: config)
  let taskA = Task { try await sessionA.run() }
  let taskB = Task { try await sessionB.run() }
  defer {
    taskA.cancel()
    taskB.cancel()
  }
  try await body(sessionA, sessionB)
  await sessionA.close()
  await sessionB.close()
}

// MARK: - InMemoryConnection Tests

@Suite("InMemoryConnection")
struct InMemoryConnectionTests {
  @Test("Write/read round-trip")
  func writeReadRoundTrip() async throws {
    let (a, b) = InMemoryConnection.makePair()
    try await a.write(contentsOf: [1, 2, 3, 4, 5])
    let result = try await readBytes(from: b, maxCount: 10)
    #expect(result == [1, 2, 3, 4, 5])
  }

  @Test("Cross-wired")
  func crossWired() async throws {
    let (a, b) = InMemoryConnection.makePair()
    try await a.write(contentsOf: [10, 20])
    try await b.write(contentsOf: [30, 40])
    #expect(try await readBytes(from: b, maxCount: 10) == [10, 20])
    #expect(try await readBytes(from: a, maxCount: 10) == [30, 40])
  }

  @Test("EOF on close")
  func eofOnClose() async throws {
    let (a, b) = InMemoryConnection.makePair()
    await a.close()
    let result = try await readBytes(from: b, maxCount: 10)
    #expect(result.isEmpty)
  }

  @Test("Read blocks until data available")
  func readBlocks() async throws {
    let (a, b) = InMemoryConnection.makePair()
    let task = Task { try await readBytes(from: b, maxCount: 10) }
    try await Task.sleep(for: .milliseconds(50))
    try await a.write(contentsOf: [42])
    #expect(try await task.value == [42])
  }

  @Test("Multiple writes coalesce")
  func multipleWrites() async throws {
    let (a, b) = InMemoryConnection.makePair()
    try await a.write(contentsOf: [1, 2])
    try await a.write(contentsOf: [3, 4])
    try await Task.sleep(for: .milliseconds(10))
    #expect(try await readBytes(from: b, maxCount: 10) == [1, 2, 3, 4])
  }

  @Test("Read respects buffer size limit")
  func readLimit() async throws {
    let (a, b) = InMemoryConnection.makePair()
    try await a.write(contentsOf: [1, 2, 3, 4, 5])
    #expect(try await readBytes(from: b, maxCount: 2) == [1, 2])
    #expect(try await readBytes(from: b, maxCount: 10) == [3, 4, 5])
  }

  @Test("Empty write is no-op")
  func emptyWrite() async throws {
    let (a, _) = InMemoryConnection.makePair()
    try await a.write(contentsOf: [])
  }
}

// MARK: - Stream Lifecycle

@Suite("Stream lifecycle")
struct StreamLifecycleTests {
  @Test("Open, send data, FIN")
  func openSendFin() async throws {
    try await withSessionPair { a, b in
      let stream = try await a.open()
      #expect(stream.id == 1)

      // Accept
      var iter = b.inbound.makeAsyncIterator()
      let remote = await iter.next()
      #expect(remote != nil)
      #expect(remote!.id == 1)

      // Send + FIN
      try await stream.write([10, 20, 30])
      try await stream.finish()

      // Read
      var received: [UInt8] = []
      for await chunk in await remote!.bytes {
        received.append(contentsOf: chunk)
      }
      #expect(received == [10, 20, 30])
      try await remote!.finish()
    }
  }

  @Test("Stream IDs: initiator=odd, responder=even")
  func streamIDs() async throws {
    try await withSessionPair { a, b in
      let s1 = try await a.open()
      let s2 = try await a.open()
      #expect(s1.id == 1)
      #expect(s2.id == 3)

      let s3 = try await b.open()
      #expect(s3.id == 2)
    }
  }

  @Test("Stream reset (RST)")
  func streamReset() async throws {
    try await withSessionPair { a, b in
      let stream = try await a.open()
      var iter = b.inbound.makeAsyncIterator()
      let remote = await iter.next()
      #expect(remote != nil)

      try await Task.sleep(for: .milliseconds(50))
      try await stream.reset()
      try await Task.sleep(for: .milliseconds(100))

      // Remote bytes should end
      var chunks: [[UInt8]] = []
      for await chunk in await remote!.bytes {
        chunks.append(chunk)
      }
      #expect(chunks.isEmpty)

      // Write after reset fails
      await #expect(throws: MuxError.self) {
        try await stream.write([1])
      }
    }
  }

  @Test("Bidirectional data on single stream")
  func bidirectional() async throws {
    try await withSessionPair { a, b in
      let streamA = try await a.open()
      var iter = b.inbound.makeAsyncIterator()
      let streamB = await iter.next()!

      // A → B
      try await streamA.write([1, 2, 3])
      try await streamA.finish()
      // B → A
      try await streamB.write([4, 5, 6])
      try await streamB.finish()

      var bReceived: [UInt8] = []
      for await chunk in await streamB.bytes {
        bReceived.append(contentsOf: chunk)
      }
      #expect(bReceived == [1, 2, 3])

      var aReceived: [UInt8] = []
      for await chunk in await streamA.bytes {
        aReceived.append(contentsOf: chunk)
      }
      #expect(aReceived == [4, 5, 6])
    }
  }
}

// MARK: - Flow Control

@Suite("Flow control")
struct FlowControlTests {
  @Test("Default window is 256KB")
  func defaultWindow() {
    #expect(MuxConfig.default.initialWindowSize == 256 * 1024)
  }

  @Test("Large write is fragmented")
  func fragmentation() async throws {
    let config = MuxConfig(
      initialWindowSize: 256 * 1024,
      maxFramePayloadSize: 16,
      keepaliveInterval: nil,
    )
    try await withSessionPair(config: config) { a, b in
      let stream = try await a.open()
      var iter = b.inbound.makeAsyncIterator()
      let remote = await iter.next()!

      let data = [UInt8](repeating: 0xBB, count: 48)
      try await stream.write(data)
      try await stream.finish()

      var received: [UInt8] = []
      for await chunk in await remote.bytes {
        #expect(chunk.count <= 16)
        received.append(contentsOf: chunk)
      }
      #expect(received == data)
    }
  }

  @Test("Small window with active consumer keeps flowing")
  func smallWindow() async throws {
    let config = MuxConfig(
      initialWindowSize: 64,
      maxFramePayloadSize: 32,
      keepaliveInterval: nil,
    )
    try await withSessionPair(config: config) { a, b in
      let stream = try await a.open()
      var iter = b.inbound.makeAsyncIterator()
      let remote = await iter.next()!

      let receiveTask = Task { () -> [UInt8] in
        var r: [UInt8] = []
        for await chunk in await remote.bytes {
          r.append(contentsOf: chunk)
        }
        return r
      }

      let data = [UInt8](repeating: 0xCC, count: 128)
      try await stream.write(data)
      try await stream.finish()

      #expect(try await receiveTask.value == data)
    }
  }
}

// MARK: - GoAway

@Suite("GoAway")
struct GoAwayTests {
  @Test("GoAway rejects new opens")
  func rejectsNew() async throws {
    try await withSessionPair { a, _ in
      try await a.goAway()
      try await Task.sleep(for: .milliseconds(100))
      await #expect(throws: MuxError.self) {
        _ = try await a.open()
      }
    }
  }

  @Test("In-flight streams complete after GoAway")
  func inflightComplete() async throws {
    try await withSessionPair { a, b in
      let stream = try await a.open()
      var iter = b.inbound.makeAsyncIterator()
      let remote = await iter.next()!

      try await a.goAway()
      try await Task.sleep(for: .milliseconds(50))

      // In-flight should still work
      try await stream.write([99])
      try await stream.finish()

      var received: [UInt8] = []
      for await chunk in await remote.bytes {
        received.append(contentsOf: chunk)
      }
      #expect(received == [99])
      try await remote.finish()
    }
  }
}

// MARK: - Keepalive

@Suite("Keepalive")
struct KeepaliveTests {
  @Test("Ping/pong keeps session alive")
  func pingPong() async throws {
    let config = MuxConfig(
      keepaliveInterval: .milliseconds(100),
      keepaliveTimeout: .milliseconds(200),
    )
    let (connA, connB) = InMemoryConnection.makePair()
    let a = MuxSession(connection: connA, role: .initiator, config: config)
    let b = MuxSession(connection: connB, role: .responder, config: config)
    let tA = Task { try await a.run() }
    let tB = Task { try await b.run() }

    try await Task.sleep(for: .milliseconds(500))
    let stream = try await a.open()
    #expect(stream.id == 1)

    await a.close()
    await b.close()
    tA.cancel()
    tB.cancel()
  }

  @Test("Keepalive timeout tears down session")
  func timeout() async {
    let config = MuxConfig(
      keepaliveInterval: .milliseconds(50),
      keepaliveTimeout: .milliseconds(50),
    )
    let (connA, connB) = InMemoryConnection.makePair()
    let a = MuxSession(connection: connA, role: .initiator, config: config)

    // Drain connB so writes don't block, but never respond to pings
    let drain = Task {
      let buf = UnsafeMutableRawBufferPointer.allocate(byteCount: 4096, alignment: 1)
      defer { buf.deallocate() }
      while true {
        let n = try await connB.read(into: buf)
        if n == 0 { break }
      }
    }

    let result = Task { () -> MuxError? in
      do {
        try await a.run()
        return nil
      } catch let e as MuxError {
        return e
      } catch {
        return nil
      }
    }

    #expect(await result.value == .keepaliveTimeout)
    drain.cancel()
    await connB.close()
  }
}

// MARK: - Concurrency

@Suite("Concurrency")
struct ConcurrencyTests {
  @Test("Multiple streams send concurrently with per-stream ordering")
  func multiStream() async throws {
    let config = MuxConfig(keepaliveInterval: nil)
    try await withSessionPair(config: config) { a, b in
      let n = 5
      let msgSize = 64

      var streams: [MuxStream] = []
      for _ in 0 ..< n {
        try await streams.append(a.open())
      }

      // Collect on B
      let recvTask = Task { () -> [UInt32: [UInt8]] in
        var results: [UInt32: [UInt8]] = [:]
        var tasks: [Task<(UInt32, [UInt8]), Never>] = []
        var count = 0
        for await s in b.inbound {
          let t = Task { () -> (UInt32, [UInt8]) in
            var d: [UInt8] = []
            for await chunk in await s.bytes {
              d.append(contentsOf: chunk)
            }
            return (s.id, d)
          }
          tasks.append(t)
          count += 1
          if count >= n { break }
        }
        for t in tasks {
          let (id, data) = await t.value
          results[id] = data
        }
        return results
      }

      // Write concurrently
      try await withThrowingTaskGroup(of: Void.self) { group in
        for (i, s) in streams.enumerated() {
          group.addTask {
            try await s.write([UInt8](repeating: UInt8(i), count: msgSize))
            try await s.finish()
          }
        }
        try await group.waitForAll()
      }

      let results = try await recvTask.value
      for (i, s) in streams.enumerated() {
        #expect(results[s.id] == [UInt8](repeating: UInt8(i), count: msgSize))
      }
    }
  }

  @Test("Open from both sides simultaneously")
  func bothSides() async throws {
    try await withSessionPair { a, b in
      async let s1 = a.open()
      async let s2 = b.open()
      let stream1 = try await s1
      let stream2 = try await s2
      #expect(stream1.id % 2 == 1)
      #expect(stream2.id % 2 == 0)
    }
  }
}

// MARK: - Error Handling

@Suite("Error handling")
struct ErrorHandlingTests {
  @Test("Write after finish throws streamClosed")
  func writeAfterFinish() async throws {
    try await withSessionPair { a, _ in
      let stream = try await a.open()
      try await stream.finish()
      await #expect(throws: MuxError.self) {
        try await stream.write([1])
      }
    }
  }

  @Test("Write after reset throws streamReset")
  func writeAfterReset() async throws {
    try await withSessionPair { a, _ in
      let stream = try await a.open()
      try await stream.reset()
      await #expect(throws: MuxError.self) {
        try await stream.write([1])
      }
    }
  }

  @Test("Open after close throws sessionClosed")
  func openAfterClose() async throws {
    let (connA, connB) = InMemoryConnection.makePair()
    let a = MuxSession(connection: connA, role: .initiator, config: MuxConfig(keepaliveInterval: nil))
    _ = MuxSession(connection: connB, role: .responder, config: MuxConfig(keepaliveInterval: nil))
    await a.close()
    await #expect(throws: MuxError.self) {
      _ = try await a.open()
    }
  }
}

// MARK: - Config

@Suite("MuxConfig")
struct MuxConfigTests {
  @Test("Default values")
  func defaults() {
    let c = MuxConfig.default
    #expect(c.initialWindowSize == 256 * 1024)
    #expect(c.maxFramePayloadSize == 64 * 1024)
    #expect(c.keepaliveInterval == .seconds(30))
    #expect(c.keepaliveTimeout == .seconds(10))
    #expect(c.maxConcurrentStreams == 0)
  }

  @Test("Custom values")
  func custom() {
    let c = MuxConfig(
      initialWindowSize: 1024,
      maxFramePayloadSize: 512,
      keepaliveInterval: .seconds(5),
      keepaliveTimeout: .seconds(2),
      maxConcurrentStreams: 100,
    )
    #expect(c.initialWindowSize == 1024)
    #expect(c.maxFramePayloadSize == 512)
  }
}
