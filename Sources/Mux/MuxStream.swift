/// A single multiplexed stream within a `MuxSession`.
///
/// Supports reading via `AsyncSequence`, writing with flow control,
/// half-close (FIN), and abort (RST).
public struct MuxStream: Sendable {
  // TODO: Implement stream state machine.

  /// The stream ID.
  public let id: UInt32

  /// Write data to the stream. Suspends if the flow control window is exhausted.
  public func write(_ data: [UInt8]) async throws {
    fatalError("TODO: implement")
  }

  /// Signal that no more data will be written (send FIN).
  public func finish() async throws {
    fatalError("TODO: implement")
  }

  /// Abort the stream immediately (send RST).
  public func reset() async throws {
    fatalError("TODO: implement")
  }
}
