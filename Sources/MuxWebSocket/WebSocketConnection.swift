#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

import Mux
import NIOCore
import HummingbirdWebSocket
import HummingbirdWSClient

/// A `Connection` backed by a WebSocket.
///
/// Each WebSocket binary message is treated as a chunk of the byte stream.
/// yamux frames are carried inside WebSocket binary frames.
public final class WebSocketConnection: Connection, @unchecked Sendable {
  private let inbound: WebSocketInboundStream
  private let outbound: WebSocketOutboundWriter
  private let readState: _WSReadState

  public init(inbound: WebSocketInboundStream, outbound: WebSocketOutboundWriter) {
    self.inbound = inbound
    self.outbound = outbound
    self.readState = _WSReadState(inbound: inbound)
  }

  public func read(into buffer: UnsafeMutableRawBufferPointer) async throws -> Int {
    try await readState.read(into: buffer)
  }

  public func write(contentsOf bytes: [UInt8]) async throws {
    let buf = ByteBuffer(bytes: bytes)
    try await outbound.write(.binary(buf))
  }

  public func close() async {
    try? await outbound.close(.normalClosure, reason: nil)
  }
}

// MARK: - Read State

/// Bridges WebSocket's frame-based inbound to Connection's byte-based reads.
final class _WSReadState: @unchecked Sendable {
  private var iterator: WebSocketInboundStream.AsyncIterator?
  private var leftover = ByteBuffer()
  private var eof = false

  init(inbound: WebSocketInboundStream) {
    self.iterator = inbound.makeAsyncIterator()
  }

  func read(into buffer: UnsafeMutableRawBufferPointer) async throws -> Int {
    // Drain leftover first
    if leftover.readableBytes > 0 {
      let count = min(leftover.readableBytes, buffer.count)
      leftover.readWithUnsafeReadableBytes { src in
        buffer.copyBytes(from: UnsafeRawBufferPointer(rebasing: src.prefix(count)))
        return count
      }
      return count
    }

    if eof { return 0 }
    guard var iter = iterator else { return 0 }

    // Read next WebSocket data frame
    guard let frame = try await iter.next() else {
      eof = true
      iterator = iter
      return 0
    }
    iterator = iter

    // frame is a WebSocketDataFrame with .data (ByteBuffer) and .opcode
    var data = frame.data
    let count = min(data.readableBytes, buffer.count)
    data.readWithUnsafeReadableBytes { src in
      buffer.copyBytes(from: UnsafeRawBufferPointer(rebasing: src.prefix(count)))
      return count
    }
    if data.readableBytes > 0 {
      leftover = data
    }
    return count
  }
}
