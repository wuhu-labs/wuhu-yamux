// swift-tools-version: 6.2
import PackageDescription

let strictConcurrency: [SwiftSetting] = [
  .unsafeFlags([
    "-Xfrontend",
    "-strict-concurrency=complete",
    "-Xfrontend",
    "-warn-concurrency",
  ]),
]

let package = Package(
  name: "wuhu-yamux",
  platforms: [
    .macOS(.v14),
    .iOS(.v16),
  ],
  products: [
    .library(name: "Mux", targets: ["Mux"]),
    .library(name: "MuxSocket", targets: ["MuxSocket"]),
    .library(name: "MuxWebSocket", targets: ["MuxWebSocket"]),
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio.git", from: "2.65.0"),
    .package(url: "https://github.com/hummingbird-project/swift-websocket.git", from: "1.0.0"),
    // Test-only: Hummingbird provides a convenient WebSocket server for integration tests.
    // Not used by any library targets.
    .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.0.0"),
    .package(url: "https://github.com/hummingbird-project/hummingbird-websocket.git", from: "2.0.0"),
    // Pin swift-collections < 1.4.0 to work around Hummingbird's missing
    // `import DequeModule` — see https://github.com/hummingbird-project/hummingbird/issues/791
    .package(url: "https://github.com/apple/swift-collections.git", "1.0.0" ..< "1.4.0"),
  ],
  targets: [
    // MARK: - Core protocol (zero external dependencies)

    .target(
      name: "Mux",
      swiftSettings: strictConcurrency,
    ),

    // MARK: - TCP / Unix domain socket transport (SwiftNIO)

    .target(
      name: "MuxSocket",
      dependencies: [
        "Mux",
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOPosix", package: "swift-nio"),
      ],
      swiftSettings: strictConcurrency,
    ),

    // MARK: - WebSocket transport (swift-websocket)

    .target(
      name: "MuxWebSocket",
      dependencies: [
        "Mux",
        .product(name: "WSCore", package: "swift-websocket"),
      ],
      swiftSettings: strictConcurrency,
    ),

    // MARK: - Tests

    .testTarget(
      name: "MuxTests",
      dependencies: ["Mux"],
      swiftSettings: strictConcurrency,
    ),
    .testTarget(
      name: "MuxSocketTests",
      dependencies: [
        "Mux",
        "MuxSocket",
      ],
      swiftSettings: strictConcurrency,
    ),
    .testTarget(
      name: "MuxWebSocketTests",
      dependencies: [
        "Mux",
        "MuxSocket",
        "MuxWebSocket",
        .product(name: "Hummingbird", package: "hummingbird"),
        .product(name: "HummingbirdWebSocket", package: "hummingbird-websocket"),
        .product(name: "WSClient", package: "swift-websocket"),
      ],
      swiftSettings: strictConcurrency,
    ),
  ],
)
