/// A multiplexed session over a `Connection`.
///
/// Both sides are peers — either side can open or accept streams.
/// The session manages the yamux state machine: stream table, flow control
/// windows, keepalive timer, GoAway state.
public struct MuxSession: Sendable {
  internal let state: _SessionState

  public init(connection: any Connection, role: MuxSessionRole, config: MuxConfig = .default) {
    self.state = _SessionState(connection: connection, role: role, config: config)
  }

  /// Run the session (read loop + keepalive). Call this in a long-lived task.
  /// Returns when the connection closes or GoAway completes.
  public func run() async throws {
    try await state.run()
  }

  /// Open a new outbound stream.
  public func open() async throws -> MuxStream {
    try await state.openStream()
  }

  /// Accept inbound streams opened by the remote peer.
  public var inbound: AsyncStream<MuxStream> {
    state.inboundStream
  }

  /// Initiate graceful shutdown. No new streams accepted after this.
  public func goAway() async throws {
    try await state.sendGoAway()
  }

  /// Forcefully close the session and all streams.
  public func close() async {
    await state.forceClose()
  }
}

// MARK: - Session State Actor

internal actor _SessionState {
  let connection: any Connection
  let role: MuxSessionRole
  let config: MuxConfig

  private var nextStreamID: UInt32
  private var streams: [UInt32: _StreamState] = [:]
  private var goAwaySent = false
  private var goAwayReceived = false
  private var isClosed = false

  // Inbound stream delivery
  private let _inboundContinuation: AsyncStream<MuxStream>.Continuation
  let inboundStream: AsyncStream<MuxStream>

  // Keepalive
  private var pendingPingID: UInt32? = nil

  init(connection: any Connection, role: MuxSessionRole, config: MuxConfig) {
    self.connection = connection
    self.role = role
    self.config = config
    self.nextStreamID = role == .initiator ? 1 : 2

    let (stream, continuation) = AsyncStream<MuxStream>.makeStream()
    self.inboundStream = stream
    self._inboundContinuation = continuation
  }

  // MARK: - Run

  nonisolated func run() async throws {
    try await withThrowingDiscardingTaskGroup { group in
      group.addTask {
        try await self.readLoop()
      }
      if self.config.keepaliveInterval != nil {
        group.addTask {
          try await self.keepaliveLoop()
        }
      }
    }
    await teardown()
  }

  // MARK: - Read Loop

  private nonisolated func readLoop() async throws {
    while !Task.isCancelled {
      guard let headerBytes = try await readExact(count: Frame.headerSize) else {
        return // EOF — clean shutdown
      }
      let frame = try Frame.decode(from: headerBytes)

      var payload: [UInt8]? = nil
      if frame.type == .data && frame.length > 0 {
        guard let data = try await readExact(count: Int(frame.length)) else {
          throw MuxError.connectionLost
        }
        payload = data
      }

      try await handleFrame(frame, payload: payload)

      if await shouldStop() { return }
    }
  }

  private nonisolated func readExact(count: Int) async throws -> [UInt8]? {
    var result = [UInt8]()
    result.reserveCapacity(count)
    let buf = UnsafeMutableRawBufferPointer.allocate(byteCount: count, alignment: 1)
    defer { buf.deallocate() }

    var totalRead = 0
    while totalRead < count {
      let slice = UnsafeMutableRawBufferPointer(
        start: buf.baseAddress! + totalRead,
        count: count - totalRead
      )
      let n = try await connection.read(into: slice)
      if n == 0 {
        if totalRead == 0 { return nil }
        throw MuxError.connectionLost
      }
      totalRead += n
    }
    return Array(UnsafeRawBufferPointer(rebasing: buf.prefix(totalRead)))
  }

  // MARK: - Frame Handling

  private func handleFrame(_ frame: Frame, payload: [UInt8]?) async throws {
    switch frame.type {
    case .data:
      try await handleData(frame, payload: payload)
    case .windowUpdate:
      try await handleWindowUpdate(frame)
    case .ping:
      try await handlePing(frame)
    case .goAway:
      handleGoAwayReceived(frame)
    }
  }

  private func handleData(_ frame: Frame, payload: [UInt8]?) async throws {
    let sid = frame.streamID

    if frame.flags.contains(.syn) {
      try await acceptInboundStream(id: sid)
    }

    guard let ss = streams[sid] else { return }

    if let data = payload, !data.isEmpty {
      if let windowUpdate = await ss.receiveData(data) {
        try? await writeFrameInternal(windowUpdate, payload: nil)
      }
    }
    if frame.flags.contains(.fin) {
      await ss.receiveFinish()
      await maybeRemoveStream(sid)
    }
    if frame.flags.contains(.rst) {
      await ss.receiveReset()
      streams.removeValue(forKey: sid)
    }
  }

  private func handleWindowUpdate(_ frame: Frame) async throws {
    let sid = frame.streamID

    if frame.flags.contains(.syn) {
      try await acceptInboundStream(id: sid)
    }

    // ACK flag — the remote accepted our outbound stream. Nothing to do
    // (we already started using the stream optimistically per yamux spec).

    guard let ss = streams[sid] else { return }

    if frame.length > 0 {
      await ss.receiveSendWindowUpdate(frame.length)
    }
    if frame.flags.contains(.fin) {
      await ss.receiveFinish()
      await maybeRemoveStream(sid)
    }
    if frame.flags.contains(.rst) {
      await ss.receiveReset()
      streams.removeValue(forKey: sid)
    }
  }

  private func handlePing(_ frame: Frame) async throws {
    if frame.flags.contains(.syn) {
      // Ping request → send pong
      let pong = Frame(type: .ping, flags: .ack, streamID: 0, length: frame.length)
      try await writeFrameInternal(pong, payload: nil)
    } else if frame.flags.contains(.ack) {
      if pendingPingID == frame.length {
        pendingPingID = nil
      }
    }
  }

  private func handleGoAwayReceived(_ frame: Frame) {
    goAwayReceived = true
    _inboundContinuation.finish()
  }

  private func acceptInboundStream(id: UInt32) async throws {
    guard !goAwayReceived, !goAwaySent, !isClosed else { return }
    guard streams[id] == nil else { return } // Already exists

    let ss = _StreamState(streamID: id, initialWindowSize: config.initialWindowSize)
    streams[id] = ss
    let muxStream = MuxStream(id: id, state: ss, session: self)

    // Send ACK
    let ack = Frame(type: .windowUpdate, flags: .ack, streamID: id, length: 0)
    try await writeFrameInternal(ack, payload: nil)

    _inboundContinuation.yield(muxStream)
  }

  // MARK: - Keepalive

  private nonisolated func keepaliveLoop() async throws {
    let interval = config.keepaliveInterval!
    let timeout = config.keepaliveTimeout

    while !Task.isCancelled {
      try await Task.sleep(for: interval)
      if await isClosed { return }

      let pingID = UInt32.random(in: 1 ... UInt32.max)
      await setPendingPing(pingID)

      let ping = Frame(type: .ping, flags: .syn, streamID: 0, length: pingID)
      try await writeFrameInternal(ping, payload: nil)

      try await Task.sleep(for: timeout)

      if await checkPingTimeout(pingID) {
        await forceClose()
        throw MuxError.keepaliveTimeout
      }
    }
  }

  private func setPendingPing(_ id: UInt32) { pendingPingID = id }
  private func checkPingTimeout(_ id: UInt32) -> Bool { pendingPingID == id }

  // MARK: - Open

  func openStream() async throws -> MuxStream {
    if isClosed { throw MuxError.sessionClosed }
    if goAwaySent || goAwayReceived { throw MuxError.sessionClosed }

    let sid = nextStreamID
    nextStreamID += 2

    let ss = _StreamState(streamID: sid, initialWindowSize: config.initialWindowSize)
    streams[sid] = ss

    // Send SYN synchronously before returning
    let syn = Frame(type: .windowUpdate, flags: .syn, streamID: sid, length: config.initialWindowSize)
    try await writeFrameInternal(syn, payload: nil)

    return MuxStream(id: sid, state: ss, session: self)
  }

  // MARK: - GoAway

  func sendGoAway() async throws {
    if goAwaySent { return }
    goAwaySent = true
    let frame = Frame(type: .goAway, flags: [], streamID: 0, length: GoAwayCode.normal.rawValue)
    try await writeFrameInternal(frame, payload: nil)
    _inboundContinuation.finish()
  }

  // MARK: - Close

  func forceClose() async {
    if isClosed { return }
    isClosed = true
    for (_, ss) in streams {
      await ss.failPending()
    }
    streams.removeAll()
    _inboundContinuation.finish()
    await connection.close()
  }

  private func shouldStop() -> Bool {
    if isClosed { return true }
    if goAwaySent && goAwayReceived && streams.isEmpty { return true }
    return false
  }

  private func teardown() async {
    if isClosed { return }
    isClosed = true
    for (_, ss) in streams {
      await ss.failPending()
    }
    streams.removeAll()
    _inboundContinuation.finish()
  }

  // MARK: - Write (serialized through actor)

  func writeFrameInternal(_ frame: Frame, payload: [UInt8]?) async throws {
    if isClosed { throw MuxError.sessionClosed }
    let header = frame.encodedHeader()
    if let payload, !payload.isEmpty {
      try await connection.write(contentsOf: header + payload)
    } else {
      try await connection.write(contentsOf: header)
    }
  }

  // MARK: - Stream Management

  func removeStream(_ id: UInt32) {
    streams.removeValue(forKey: id)
  }

  func maybeRemoveStream(_ id: UInt32) async {
    guard let ss = streams[id] else { return }
    let local = await ss.isLocalFinished()
    let remote = await ss.isRemoteFinished()
    if local && remote {
      streams.removeValue(forKey: id)
    }
  }
}
