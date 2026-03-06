#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

import Mux
import NIOCore
import NIOPosix

/// A `Connection` backed by a NIO `Channel` (TCP or Unix domain socket).
///
/// Created by `TCPListener` (server side) or `TCPConnector` (client side),
/// or via `TCPConnection.wrap(channel:)`.
public final class TCPConnection: Connection, @unchecked Sendable {
  private let channel: Channel
  private let readState: _ReadState

  init(channel: Channel, readState: _ReadState) {
    self.channel = channel
    self.readState = readState
  }

  public func read(into buffer: UnsafeMutableRawBufferPointer) async throws -> Int {
    try await readState.read(into: buffer)
  }

  public func write(contentsOf bytes: [UInt8]) async throws {
    let buf = channel.allocator.buffer(bytes: bytes)
    try await channel.writeAndFlush(buf)
  }

  public func close() async {
    try? await channel.close()
  }

  /// Create a `TCPConnection` from a raw NIO `Channel`.
  /// Installs the necessary channel handler.
  public static func wrap(channel: Channel) -> TCPConnection {
    let state = _ReadState()
    let handler = _MuxChannelHandler(readState: state)
    channel.pipeline.addHandler(handler).whenFailure { _ in
      state.receiveError(MuxError.connectionLost)
    }
    return TCPConnection(channel: channel, readState: state)
  }
}

// MARK: - Read State

/// Bridges NIO's push-based reads to Connection's pull-based reads.
final class _ReadState: @unchecked Sendable {
  private var buffer: ByteBuffer = ByteBuffer()
  private var eof = false
  private var error: (any Error)?
  private var waiter: CheckedContinuation<Int, any Error>?
  private var waiterBuffer: UnsafeMutableRawBufferPointer?
  private var _lock = pthread_mutex_t()

  init() { pthread_mutex_init(&_lock, nil) }
  deinit { pthread_mutex_destroy(&_lock) }
  private func lock() { pthread_mutex_lock(&_lock) }
  private func unlock() { pthread_mutex_unlock(&_lock) }

  func read(into target: UnsafeMutableRawBufferPointer) async throws -> Int {
    lock()
    if buffer.readableBytes > 0 {
      let count = min(buffer.readableBytes, target.count)
      buffer.readWithUnsafeReadableBytes { src in
        target.copyBytes(from: UnsafeRawBufferPointer(rebasing: src.prefix(count)))
        return count
      }
      unlock()
      return count
    }
    if let err = error { unlock(); throw err }
    if eof { unlock(); return 0 }
    unlock()

    return try await withCheckedThrowingContinuation { cont in
      lock()
      if buffer.readableBytes > 0 {
        let count = min(buffer.readableBytes, target.count)
        buffer.readWithUnsafeReadableBytes { src in
          target.copyBytes(from: UnsafeRawBufferPointer(rebasing: src.prefix(count)))
          return count
        }
        unlock()
        cont.resume(returning: count)
        return
      }
      if let err = error { unlock(); cont.resume(throwing: err); return }
      if eof { unlock(); cont.resume(returning: 0); return }
      waiter = cont
      waiterBuffer = target
      unlock()
    }
  }

  func receive(_ data: ByteBuffer) {
    lock()
    if let w = waiter, let wb = waiterBuffer {
      waiter = nil
      waiterBuffer = nil
      var copy = data
      let count = min(copy.readableBytes, wb.count)
      copy.readWithUnsafeReadableBytes { src in
        wb.copyBytes(from: UnsafeRawBufferPointer(rebasing: src.prefix(count)))
        return count
      }
      if copy.readableBytes > 0 { buffer.writeBuffer(&copy) }
      unlock()
      w.resume(returning: count)
    } else {
      var copy = data
      buffer.writeBuffer(&copy)
      unlock()
    }
  }

  func receiveEOF() {
    lock()
    eof = true
    if let w = waiter {
      waiter = nil; waiterBuffer = nil
      unlock()
      w.resume(returning: 0)
    } else { unlock() }
  }

  func receiveError(_ err: any Error) {
    lock()
    error = err
    if let w = waiter {
      waiter = nil; waiterBuffer = nil
      unlock()
      w.resume(throwing: err)
    } else { unlock() }
  }
}

// MARK: - NIO Channel Handler

final class _MuxChannelHandler: ChannelInboundHandler, @unchecked Sendable {
  typealias InboundIn = ByteBuffer
  let readState: _ReadState

  init(readState: _ReadState) { self.readState = readState }

  func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    readState.receive(unwrapInboundIn(data))
  }

  func channelInactive(context: ChannelHandlerContext) {
    readState.receiveEOF()
  }

  func errorCaught(context: ChannelHandlerContext, error: any Error) {
    readState.receiveError(error)
    context.close(promise: nil)
  }
}
