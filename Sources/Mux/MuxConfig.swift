/// Configuration for a `MuxSession`.
public struct MuxConfig: Sendable {
  /// Initial window size per stream (bytes). Default: 256 KB (yamux spec default).
  public var initialWindowSize: UInt32

  /// Maximum frame payload size (bytes). Default: 64 KB.
  /// Larger payloads are split across multiple Data frames.
  public var maxFramePayloadSize: UInt32

  /// Keepalive ping interval. `nil` to disable keepalive. Default: 30 seconds.
  public var keepaliveInterval: Duration?

  /// Keepalive timeout. If a pong is not received within this duration after
  /// a ping is sent, the session is torn down. Default: 10 seconds.
  public var keepaliveTimeout: Duration

  /// Maximum number of concurrent streams. 0 = unlimited. Default: 0.
  public var maxConcurrentStreams: UInt32

  /// The default configuration.
  public static let `default` = MuxConfig()

  public init(
    initialWindowSize: UInt32 = 256 * 1024,
    maxFramePayloadSize: UInt32 = 64 * 1024,
    keepaliveInterval: Duration? = .seconds(30),
    keepaliveTimeout: Duration = .seconds(10),
    maxConcurrentStreams: UInt32 = 0
  ) {
    self.initialWindowSize = initialWindowSize
    self.maxFramePayloadSize = maxFramePayloadSize
    self.keepaliveInterval = keepaliveInterval
    self.keepaliveTimeout = keepaliveTimeout
    self.maxConcurrentStreams = maxConcurrentStreams
  }
}
