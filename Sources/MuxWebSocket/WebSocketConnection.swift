#if canImport(Glibc)
  import Glibc
#elseif canImport(Musl)
  import Musl
#elseif canImport(Darwin)
  import Darwin
#endif

import Mux
import NIOCore
import WSCore

/// Default maximum WebSocket frame payload size used for chunking writes.
///
/// Matches the default `maxFrameSize` in both HummingbirdWebSocket and
/// swift-websocket's `WebSocketClient` (1 << 14 = 16 384 bytes). Outgoing
/// byte sequences larger than this are split across multiple binary frames
/// so that the peer's frame decoder never rejects them.
public let defaultMaxWebSocketFrameSize = 1 << 14

/// A `Connection` backed by a WebSocket.
///
/// Each WebSocket binary message is treated as a chunk of the byte stream.
/// yamux frames are carried inside WebSocket binary frames.
///
/// Large writes are automatically chunked into multiple WebSocket frames
/// whose payload does not exceed ``maxWriteFrameSize``, preventing
/// `NIOWebSocketError.invalidFrameLength` on the receiving side.
public final class WebSocketConnection: Connection, @unchecked Sendable {
  private let inbound: WebSocketInboundStream
  private let outbound: WebSocketOutboundWriter
  private let readState: _WSReadState

  /// Maximum payload size per outgoing WebSocket binary frame.
  ///
  /// Must not exceed the peer's `maxFrameSize` (the decoder limit).
  /// Defaults to ``defaultMaxWebSocketFrameSize`` (16 KB).
  public let maxWriteFrameSize: Int

  /// Create a WebSocket-backed connection.
  ///
  /// - Parameters:
  ///   - inbound: The WebSocket inbound stream.
  ///   - outbound: The WebSocket outbound writer.
  ///   - maxWriteFrameSize: Maximum payload bytes per outgoing WebSocket
  ///     frame. Must be positive. Defaults to ``defaultMaxWebSocketFrameSize``
  ///     (16 KB), which matches the common default on both server and client
  ///     WebSocket decoders.
  public init(
    inbound: WebSocketInboundStream,
    outbound: WebSocketOutboundWriter,
    maxWriteFrameSize: Int = defaultMaxWebSocketFrameSize,
  ) {
    precondition(maxWriteFrameSize > 0, "maxWriteFrameSize must be positive")
    self.inbound = inbound
    self.outbound = outbound
    self.maxWriteFrameSize = maxWriteFrameSize
    readState = _WSReadState(inbound: inbound)
  }

  public func read(into buffer: UnsafeMutableRawBufferPointer) async throws -> Int {
    try await readState.read(into: buffer)
  }

  public func write(contentsOf bytes: [UInt8]) async throws {
    guard !bytes.isEmpty else { return }

    var offset = 0
    while offset < bytes.count {
      let end = min(offset + maxWriteFrameSize, bytes.count)
      let chunk = ByteBuffer(bytes: bytes[offset ..< end])
      try await outbound.write(.binary(chunk))
      offset = end
    }
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
    iterator = inbound.makeAsyncIterator()
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
