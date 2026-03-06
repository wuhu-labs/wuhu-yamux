import Mux
import HummingbirdWebSocket
import HummingbirdWSClient

/// A `Connection` backed by a Hummingbird WebSocket.
public final class WebSocketConnection: Connection, @unchecked Sendable {
  // TODO: Implement.

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
