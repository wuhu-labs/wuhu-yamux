/// Errors produced by the mux layer.
public enum MuxError: Error, Sendable, Equatable {
  /// The frame header is malformed or uses an unsupported version/type.
  case invalidFrame(String)

  /// The session has been closed or is shutting down via GoAway.
  case sessionClosed

  /// A stream operation was attempted on a stream that is already closed or reset.
  case streamClosed

  /// The remote sent a GoAway. No new streams can be opened.
  case goAway(GoAwayCode)

  /// A protocol violation was detected.
  case protocolError(String)

  /// The connection was lost unexpectedly.
  case connectionLost

  /// Keepalive timeout — the remote peer did not respond to pings.
  case keepaliveTimeout

  /// The stream was reset by the remote peer.
  case streamReset
}

/// GoAway error codes as defined by yamux.
public enum GoAwayCode: UInt32, Sendable, Equatable {
  case normal = 0x0
  case protocolError = 0x1
  case internalError = 0x2
}
