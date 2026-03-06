/// A multiplexed session over a `Connection`.
///
/// Both sides are peers — either side can open or accept streams.
/// The session manages the yamux state machine: stream table, flow control
/// windows, keepalive timer, GoAway state.
///
/// Usage:
/// ```swift
/// let session = MuxSession(connection: conn, role: .initiator)
///
/// // Run the session in a long-lived task:
/// Task { try await session.run() }
///
/// // Open outbound streams:
/// let stream = try await session.open()
/// try await stream.write([1, 2, 3])
/// try await stream.finish()
///
/// // Accept inbound streams:
/// for await stream in session.inbound {
///   // handle stream
/// }
/// ```
public struct MuxSession: Sendable {
  // TODO: Implement session state machine.

  let connection: any Connection
  let role: MuxSessionRole
  let config: MuxConfig

  public init(connection: any Connection, role: MuxSessionRole, config: MuxConfig = .default) {
    self.connection = connection
    self.role = role
    self.config = config
  }

  /// Run the session (read loop + keepalive). Call this in a long-lived task.
  /// Returns when the connection closes or GoAway completes.
  public func run() async throws {
    fatalError("TODO: implement")
  }

  /// Open a new outbound stream.
  public func open() async throws -> MuxStream {
    fatalError("TODO: implement")
  }

  /// Accept inbound streams opened by the remote peer.
  public var inbound: AsyncStream<MuxStream> {
    fatalError("TODO: implement")
  }

  /// Initiate graceful shutdown. No new streams accepted after this.
  public func goAway() async {
    fatalError("TODO: implement")
  }

  /// Forcefully close the session and all streams.
  public func close() async {
    fatalError("TODO: implement")
  }
}
