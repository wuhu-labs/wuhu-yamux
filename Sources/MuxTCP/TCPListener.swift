import Mux
import NIOCore
import NIOPosix

/// Listens for inbound TCP or Unix domain socket connections.
///
/// Each accepted connection is wrapped in a `TCPConnection`.
/// Typical usage:
/// ```swift
/// let listener = try await TCPListener.bind(host: "127.0.0.1", port: 8080)
/// for await connection in listener.connections {
///   let session = MuxSession(connection: connection, role: .responder)
///   Task { try await session.run() }
/// }
/// ```
public struct TCPListener: Sendable {
  private let serverChannel: Channel
  private let _connections: AsyncStream<TCPConnection>

  /// Accepted connections as an async sequence.
  public var connections: AsyncStream<TCPConnection> { _connections }

  /// The local address the listener is bound to.
  public var localAddress: SocketAddress? { serverChannel.localAddress }

  /// Bind to a TCP address.
  public static func bind(
    host: String,
    port: Int,
    eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton
  ) async throws -> TCPListener {
    let (stream, continuation) = AsyncStream<TCPConnection>.makeStream()

    let bootstrap = ServerBootstrap(group: eventLoopGroup)
      .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
      .childChannelInitializer { channel in
        channel.eventLoop.makeSucceededVoidFuture().map {
          let conn = TCPConnection.wrap(channel: channel)
          continuation.yield(conn)
        }
      }

    let serverChan = try await bootstrap.bind(host: host, port: port).get()

    serverChan.closeFuture.whenComplete { _ in
      continuation.finish()
    }

    return TCPListener(serverChannel: serverChan, _connections: stream)
  }

  /// Bind to a Unix domain socket path.
  public static func bind(
    unixDomainSocketPath path: String,
    eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton
  ) async throws -> TCPListener {
    let (stream, continuation) = AsyncStream<TCPConnection>.makeStream()

    let bootstrap = ServerBootstrap(group: eventLoopGroup)
      .childChannelInitializer { channel in
        channel.eventLoop.makeSucceededVoidFuture().map {
          let conn = TCPConnection.wrap(channel: channel)
          continuation.yield(conn)
        }
      }

    let serverChan = try await bootstrap.bind(unixDomainSocketPath: path).get()

    serverChan.closeFuture.whenComplete { _ in
      continuation.finish()
    }

    return TCPListener(serverChannel: serverChan, _connections: stream)
  }

  /// Stop accepting new connections.
  public func close() async {
    try? await serverChannel.close()
  }
}
