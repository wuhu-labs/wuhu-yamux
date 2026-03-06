// MARK: - Connection Protocol

/// A reliable, ordered, bidirectional byte stream.
///
/// Transports (TCP, WebSocket, in-memory pipe) conform to this protocol.
/// The mux layer reads and writes bytes through it without knowing the
/// underlying transport.
public protocol Connection: Sendable {
  /// Read bytes into the buffer. Returns the number of bytes read. 0 = EOF.
  func read(into buffer: UnsafeMutableRawBufferPointer) async throws -> Int

  /// Write all bytes to the connection.
  func write(contentsOf bytes: [UInt8]) async throws

  /// Close the connection.
  func close() async
}
