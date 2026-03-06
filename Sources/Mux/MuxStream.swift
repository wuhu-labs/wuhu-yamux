/// A single multiplexed stream within a `MuxSession`.
///
/// Supports reading via `AsyncSequence`, writing with flow control,
/// half-close (FIN), and abort (RST).
public struct MuxStream: Sendable {
  /// The stream ID.
  public let id: UInt32

  internal let _state: _StreamState
  internal let _session: _SessionState

  internal init(id: UInt32, state: _StreamState, session: _SessionState) {
    self.id = id
    self._state = state
    self._session = session
  }

  /// An `AsyncSequence` of `[UInt8]` data chunks received on this stream.
  /// Terminates when FIN is received or the stream is reset.
  public var bytes: AsyncStream<[UInt8]> {
    get async {
      await _state.getOrMakeBytesStream()
    }
  }

  /// Write data to the stream. Suspends if the flow control window is exhausted.
  public func write(_ data: [UInt8]) async throws {
    guard !data.isEmpty else { return }
    if await _state.isLocalFinished() { throw MuxError.streamClosed }
    if await _state.isReset() { throw MuxError.streamReset }

    let maxPayload = _session.config.maxFramePayloadSize

    var offset = 0
    while offset < data.count {
      let end = min(offset + Int(maxPayload), data.count)
      let chunk = Array(data[offset ..< end])
      let size = UInt32(chunk.count)

      // Wait for flow control window
      try await _state.acquireSendWindow(size)
      if await _state.isReset() { throw MuxError.streamReset }

      let frame = Frame(type: .data, flags: [], streamID: id, length: size)
      try await _session.writeFrameInternal(frame, payload: chunk)
      offset = end
    }
  }

  /// Signal that no more data will be written (send FIN).
  public func finish() async throws {
    if await _state.setLocalFinished() { return } // already finished

    let frame = Frame(type: .data, flags: .fin, streamID: id, length: 0)
    try await _session.writeFrameInternal(frame, payload: nil)
    await _session.maybeRemoveStream(id)
  }

  /// Abort the stream immediately (send RST).
  public func reset() async throws {
    if await _state.setReset() { return } // already reset

    let frame = Frame(type: .data, flags: .rst, streamID: id, length: 0)
    try await _session.writeFrameInternal(frame, payload: nil)
    await _state.failPending()
    await _session.removeStream(id)
  }
}

// MARK: - Stream State Actor

internal actor _StreamState {
  let streamID: UInt32
  let initialWindowSize: UInt32

  private var sendWindow: Int64
  private var localFinished = false
  private var remoteFinished = false
  private var isResetFlag = false

  // Read side — buffered chunks + AsyncStream delivery
  private var bufferedChunks: [[UInt8]] = []
  private var readContinuation: AsyncStream<[UInt8]>.Continuation?
  private var readStream: AsyncStream<[UInt8]>?
  private var readStreamAttached = false

  // Write side — flow control waiters
  private var sendWaiters: [(needed: UInt32, continuation: CheckedContinuation<Void, any Error>)] = []

  // Receive window tracking for sending window updates
  private var recvConsumed: UInt32 = 0

  init(streamID: UInt32, initialWindowSize: UInt32) {
    self.streamID = streamID
    self.initialWindowSize = initialWindowSize
    self.sendWindow = Int64(initialWindowSize)
  }

  func isLocalFinished() -> Bool { localFinished }
  func isRemoteFinished() -> Bool { remoteFinished }
  func isReset() -> Bool { isResetFlag }

  /// Returns true if was already finished.
  func setLocalFinished() -> Bool {
    if localFinished { return true }
    localFinished = true
    return false
  }

  /// Returns true if was already reset.
  func setReset() -> Bool {
    if isResetFlag { return true }
    isResetFlag = true
    return false
  }

  // MARK: - Read Side

  func getOrMakeBytesStream() -> AsyncStream<[UInt8]> {
    if let existing = readStream { return existing }

    let (stream, continuation) = AsyncStream<[UInt8]>.makeStream()
    readContinuation = continuation
    readStream = stream
    readStreamAttached = true

    // Flush any buffered chunks
    for chunk in bufferedChunks {
      continuation.yield(chunk)
    }
    bufferedChunks.removeAll()

    // If already finished, close immediately
    if remoteFinished || isResetFlag {
      continuation.finish()
      readContinuation = nil
    }

    return stream
  }

  /// Receive data from the remote peer. Returns an optional window update
  /// frame that the caller (the session's read loop) must send.
  func receiveData(_ data: [UInt8]) -> Frame? {
    recvConsumed += UInt32(data.count)

    if readStreamAttached, let cont = readContinuation {
      cont.yield(data)
    } else {
      bufferedChunks.append(data)
    }

    // Send window update when we've consumed >= half the initial window
    if recvConsumed >= initialWindowSize / 2 {
      let delta = recvConsumed
      recvConsumed = 0
      return Frame(type: .windowUpdate, flags: [], streamID: streamID, length: delta)
    }
    return nil
  }

  func receiveFinish() {
    remoteFinished = true
    readContinuation?.finish()
    readContinuation = nil
  }

  func receiveReset() {
    isResetFlag = true
    readContinuation?.finish()
    readContinuation = nil
    let waiters = sendWaiters
    sendWaiters.removeAll()
    for w in waiters {
      w.continuation.resume(throwing: MuxError.streamReset)
    }
  }

  func failPending() {
    readContinuation?.finish()
    readContinuation = nil
    let waiters = sendWaiters
    sendWaiters.removeAll()
    for w in waiters {
      w.continuation.resume(throwing: MuxError.sessionClosed)
    }
  }

  // MARK: - Write Side (flow control)

  func acquireSendWindow(_ needed: UInt32) async throws {
    if isResetFlag { throw MuxError.streamReset }
    if sendWindow >= Int64(needed) {
      sendWindow -= Int64(needed)
      return
    }
    // Wait for window update
    try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, any Error>) in
      if isResetFlag {
        cont.resume(throwing: MuxError.streamReset)
        return
      }
      sendWaiters.append((needed: needed, continuation: cont))
    }
  }

  func receiveSendWindowUpdate(_ delta: UInt32) {
    sendWindow += Int64(delta)
    var remaining: [(needed: UInt32, continuation: CheckedContinuation<Void, any Error>)] = []
    for waiter in sendWaiters {
      if sendWindow >= Int64(waiter.needed) {
        sendWindow -= Int64(waiter.needed)
        waiter.continuation.resume()
      } else {
        remaining.append(waiter)
      }
    }
    sendWaiters = remaining
  }
}
