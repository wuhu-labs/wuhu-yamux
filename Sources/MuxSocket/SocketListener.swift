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

/// Listens for inbound TCP or Unix domain socket connections.
///
/// Each accepted connection is wrapped in a `SocketConnection`.
///
/// For Unix domain sockets, the listener handles stale socket files on bind:
/// if the path already exists, it attempts a test connection. If the
/// connection is refused (stale file), the file is removed and binding
/// proceeds. If the connection succeeds (live socket), the bind fails to
/// avoid stealing another server's socket.
///
/// Usage:
/// ```swift
/// let listener = try await SocketListener.bind(host: "127.0.0.1", port: 8080)
/// for await connection in listener.connections {
///   let session = MuxSession(connection: connection, role: .responder)
///   Task { try await session.run() }
/// }
/// ```
public struct SocketListener: Sendable {
  private let serverChannel: Channel
  private let _connections: AsyncStream<SocketConnection>
  private let socketPath: String?

  /// Accepted connections as an async sequence.
  public var connections: AsyncStream<SocketConnection> {
    _connections
  }

  /// The local address the listener is bound to.
  public var localAddress: SocketAddress? {
    serverChannel.localAddress
  }

  /// Bind to a TCP address.
  public static func bind(
    host: String,
    port: Int,
    eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
  ) async throws -> SocketListener {
    let (stream, continuation) = AsyncStream<SocketConnection>.makeStream()

    let bootstrap = ServerBootstrap(group: eventLoopGroup)
      .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
      .childChannelInitializer { channel in
        channel.eventLoop.makeSucceededVoidFuture().map {
          let conn = SocketConnection.wrap(channel: channel)
          continuation.yield(conn)
        }
      }

    let serverChan = try await bootstrap.bind(host: host, port: port).get()

    serverChan.closeFuture.whenComplete { _ in
      continuation.finish()
    }

    return SocketListener(
      serverChannel: serverChan,
      _connections: stream,
      socketPath: nil,
    )
  }

  /// Bind to a Unix domain socket path.
  ///
  /// - Parameters:
  ///   - path: Filesystem path for the socket.
  ///   - cleanupStaleSocket: If `true` (default), attempt to remove a stale
  ///     socket file at `path` before binding.
  ///   - eventLoopGroup: NIO event loop group to use.
  public static func bind(
    unixDomainSocketPath path: String,
    cleanupStaleSocket: Bool = true,
    eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
  ) async throws -> SocketListener {
    if cleanupStaleSocket {
      try await cleanupStaleSocketFile(at: path, eventLoopGroup: eventLoopGroup)
    }

    let (stream, continuation) = AsyncStream<SocketConnection>.makeStream()

    let bootstrap = ServerBootstrap(group: eventLoopGroup)
      .childChannelInitializer { channel in
        channel.eventLoop.makeSucceededVoidFuture().map {
          let conn = SocketConnection.wrap(channel: channel)
          continuation.yield(conn)
        }
      }

    let serverChan = try await bootstrap.bind(unixDomainSocketPath: path).get()

    serverChan.closeFuture.whenComplete { _ in
      continuation.finish()
    }

    return SocketListener(
      serverChannel: serverChan,
      _connections: stream,
      socketPath: path,
    )
  }

  /// Stop accepting new connections. For Unix domain sockets, also removes
  /// the socket file.
  public func close() async {
    try? await serverChannel.close()
    if let path = socketPath {
      unlink(path)
    }
  }

  // MARK: - Stale socket cleanup

  /// Check if a socket file exists at `path`. If it does, try connecting to
  /// it. If the connection is refused, the file is stale and we remove it.
  /// If the connection succeeds, someone is actively listening — throw an
  /// error rather than stealing the socket.
  private static func cleanupStaleSocketFile(
    at path: String,
    eventLoopGroup: EventLoopGroup,
  ) async throws {
    // Check if file exists
    var stat = stat()
    guard lstat(path, &stat) == 0 else {
      // File doesn't exist — nothing to clean up
      return
    }

    // Verify it's a socket file (S_ISSOCK)
    guard (stat.st_mode & S_IFMT) == S_IFSOCK else {
      throw SocketError.pathExistsButNotSocket(path)
    }

    // Try connecting to see if it's live
    do {
      let bootstrap = ClientBootstrap(group: eventLoopGroup)
      let channel = try await bootstrap.connect(unixDomainSocketPath: path).get()
      // Connection succeeded — socket is live, don't remove it
      try? await channel.close()
      throw SocketError.socketAlreadyInUse(path)
    } catch let error as SocketError {
      throw error
    } catch {
      // Connection failed — socket is stale, remove it
      unlink(path)
    }
  }
}

/// Errors specific to Unix domain socket operations.
public enum SocketError: Error, Sendable, CustomStringConvertible {
  /// The path exists but is not a socket file.
  case pathExistsButNotSocket(String)

  /// The socket path is already in use by a live listener.
  case socketAlreadyInUse(String)

  public var description: String {
    switch self {
    case let .pathExistsButNotSocket(path):
      "Path exists but is not a socket: \(path)"
    case let .socketAlreadyInUse(path):
      "Socket already in use: \(path)"
    }
  }
}
