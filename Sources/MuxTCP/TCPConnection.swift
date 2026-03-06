import Mux
import NIOCore
import NIOPosix

/// A `Connection` backed by a NIO `Channel` (TCP or Unix domain socket).
public final class TCPConnection: Connection, @unchecked Sendable {
  // TODO: Implement NIO Channel wrapper.

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

/// Accepts inbound TCP/UDS connections and yields `MuxSession` instances.
public struct TCPListener: Sendable {
  // TODO: Implement.

  public init() {
    fatalError("TODO: implement")
  }
}

/// Dials out to a remote TCP/UDS address and returns a `MuxSession`.
public struct TCPConnector: Sendable {
  // TODO: Implement.

  public init() {
    fatalError("TODO: implement")
  }
}
