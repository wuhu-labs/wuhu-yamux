import Testing
@testable import Mux

@Suite("Frame encoding/decoding")
struct FrameTests {
  @Test("Round-trip encode/decode data frame")
  func roundTripDataFrame() throws {
    let frame = Frame(type: .data, flags: .syn, streamID: 1, length: 256)
    let encoded = frame.encodedHeader()
    let decoded = try Frame.decode(from: encoded)
    #expect(decoded == frame)
  }

  @Test("Round-trip all frame types")
  func roundTripAllTypes() throws {
    let types: [FrameType] = [.data, .windowUpdate, .ping, .goAway]
    for type in types {
      let frame = Frame(type: type, flags: [], streamID: 0, length: 42)
      let encoded = frame.encodedHeader()
      let decoded = try Frame.decode(from: encoded)
      #expect(decoded == frame)
    }
  }

  @Test("Decode rejects short buffer")
  func rejectShortBuffer() {
    #expect(throws: MuxError.self) {
      _ = try Frame.decode(from: [0, 0, 0])
    }
  }

  @Test("Decode rejects unsupported version")
  func rejectBadVersion() {
    var bytes = Frame(type: .data, flags: [], streamID: 0, length: 0).encodedHeader()
    bytes[0] = 1 // bad version
    #expect(throws: MuxError.self) {
      _ = try Frame.decode(from: bytes)
    }
  }

  @Test("All flag combinations")
  func allFlagCombinations() throws {
    let flags: [FrameFlags] = [.syn, .ack, .fin, .rst, [.syn, .ack], [.fin, .rst]]
    for f in flags {
      let frame = Frame(type: .data, flags: f, streamID: 7, length: 0)
      let decoded = try Frame.decode(from: frame.encodedHeader())
      #expect(decoded.flags == f)
    }
  }

  @Test("Max stream ID and length")
  func maxValues() throws {
    let frame = Frame(type: .windowUpdate, flags: [], streamID: UInt32.max, length: UInt32.max)
    let decoded = try Frame.decode(from: frame.encodedHeader())
    #expect(decoded == frame)
  }
}
