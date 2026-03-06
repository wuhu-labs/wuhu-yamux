import Mux
import NIOCore
import NIOPosix

/// Connects to a remote TCP or Unix domain socket address.
///
/// Usage:
/// ```swift
/// let connection = try await TCPConnector.connect(host: "127.0.0.1", port: 8080)
/// let session = MuxSession(connection: connection, role: .initiator)
/// try await session.run()
/// ```
public struct TCPConnector: Sendable {
  /// Connect to a TCP address.
  public static func connect(
    host: String,
    port: Int,
    eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton
  ) async throws -> TCPConnection {
    let bootstrap = ClientBootstrap(group: eventLoopGroup)
    let channel = try await bootstrap.connect(host: host, port: port).get()
    return TCPConnection.wrap(channel: channel)
  }

  /// Connect to a Unix domain socket path.
  public static func connect(
    unixDomainSocketPath path: String,
    eventLoopGroup: EventLoopGroup = MultiThreadedEventLoopGroup.singleton
  ) async throws -> TCPConnection {
    let bootstrap = ClientBootstrap(group: eventLoopGroup)
    let channel = try await bootstrap.connect(unixDomainSocketPath: path).get()
    return TCPConnection.wrap(channel: channel)
  }
}
