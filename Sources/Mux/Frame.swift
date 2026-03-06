// MARK: - Frame Types

/// yamux frame header (12 bytes, big-endian).
///
/// ```
/// ┌─────────┬──────┬───────┬──────────┬──────────┐
/// │ Version │ Type │ Flags │ StreamID │ Length   │
/// │ 1 byte  │1 byte│2 bytes│ 4 bytes  │ 4 bytes  │
/// └─────────┴──────┴───────┴──────────┴──────────┘
/// ```
public struct Frame: Sendable, Equatable {
  /// The yamux protocol version. Always 0.
  public static let version: UInt8 = 0

  /// Header size in bytes.
  public static let headerSize = 12

  /// The frame type.
  public var type: FrameType

  /// The frame flags.
  public var flags: FrameFlags

  /// The stream ID. 0 for session-level frames (Ping, GoAway).
  public var streamID: UInt32

  /// The length/value field. Meaning depends on frame type:
  /// - Data: payload byte count
  /// - WindowUpdate: window size delta
  /// - Ping: opaque value echoed back
  /// - GoAway: error code
  public var length: UInt32

  public init(type: FrameType, flags: FrameFlags, streamID: UInt32, length: UInt32) {
    self.type = type
    self.flags = flags
    self.streamID = streamID
    self.length = length
  }
}

/// yamux frame types.
public enum FrameType: UInt8, Sendable, Equatable {
  case data = 0x0
  case windowUpdate = 0x1
  case ping = 0x2
  case goAway = 0x3
}

/// yamux frame flags (bitmask).
public struct FrameFlags: OptionSet, Sendable, Equatable {
  public let rawValue: UInt16

  public init(rawValue: UInt16) {
    self.rawValue = rawValue
  }

  /// Signals the start of a new stream.
  public static let syn = FrameFlags(rawValue: 0x1)
  /// Acknowledges the start of a new stream.
  public static let ack = FrameFlags(rawValue: 0x2)
  /// Half-close: sender will send no more data.
  public static let fin = FrameFlags(rawValue: 0x4)
  /// Hard reset: immediately close the stream.
  public static let rst = FrameFlags(rawValue: 0x8)
}

// MARK: - Encoding / Decoding

extension Frame {
  /// Encode the frame header into a 12-byte array.
  public func encodedHeader() -> [UInt8] {
    var bytes = [UInt8](repeating: 0, count: Frame.headerSize)
    bytes[0] = Frame.version
    bytes[1] = type.rawValue
    bytes[2] = UInt8(flags.rawValue >> 8)
    bytes[3] = UInt8(flags.rawValue & 0xFF)
    bytes[4] = UInt8((streamID >> 24) & 0xFF)
    bytes[5] = UInt8((streamID >> 16) & 0xFF)
    bytes[6] = UInt8((streamID >> 8) & 0xFF)
    bytes[7] = UInt8(streamID & 0xFF)
    bytes[8] = UInt8((length >> 24) & 0xFF)
    bytes[9] = UInt8((length >> 16) & 0xFF)
    bytes[10] = UInt8((length >> 8) & 0xFF)
    bytes[11] = UInt8(length & 0xFF)
    return bytes
  }

  /// Decode a frame header from a 12-byte buffer.
  /// - Throws: `MuxError.invalidFrame` if the version is unsupported or type is unknown.
  public static func decode(from bytes: [UInt8]) throws -> Frame {
    guard bytes.count >= headerSize else {
      throw MuxError.invalidFrame("Header too short: \(bytes.count) bytes")
    }
    let version = bytes[0]
    guard version == Frame.version else {
      throw MuxError.invalidFrame("Unsupported version: \(version)")
    }
    guard let type = FrameType(rawValue: bytes[1]) else {
      throw MuxError.invalidFrame("Unknown frame type: \(bytes[1])")
    }
    let flags = FrameFlags(rawValue: UInt16(bytes[2]) << 8 | UInt16(bytes[3]))
    let streamID = UInt32(bytes[4]) << 24 | UInt32(bytes[5]) << 16 | UInt32(bytes[6]) << 8 | UInt32(bytes[7])
    let length = UInt32(bytes[8]) << 24 | UInt32(bytes[9]) << 16 | UInt32(bytes[10]) << 8 | UInt32(bytes[11])
    return Frame(type: type, flags: flags, streamID: streamID, length: length)
  }
}
