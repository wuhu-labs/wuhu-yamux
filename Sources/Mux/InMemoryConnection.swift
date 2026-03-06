/// Paired in-memory connections for testing.
///
/// Two `InMemoryConnection` instances are created together and cross-wired:
/// writes to one appear as reads on the other. No I/O, no threads, fully
/// deterministic.
public final class InMemoryConnection: Connection, @unchecked Sendable {
  // TODO: Implement with AsyncStream-backed cross-wired buffers.

  /// Create a pair of connected in-memory connections.
  public static func makePair() -> (InMemoryConnection, InMemoryConnection) {
    fatalError("TODO: implement")
  }

  public func read(into buffer: UnsafeMutableRawBufferPointer) async throws -> Int {
    fatalError("TODO: implement")
  }

  public func write(contentsOf bytes: [UInt8]) async throws {
    fatalError("TODO: implement")
  }

  public func close() async {
    fatalError("TODO: implement")
  }
}
