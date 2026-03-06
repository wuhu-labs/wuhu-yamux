/// The role of a session in the yamux protocol.
///
/// Determines stream ID assignment:
/// - `.initiator` uses odd stream IDs (1, 3, 5, ...)
/// - `.responder` uses even stream IDs (2, 4, 6, ...)
///
/// This prevents stream ID collisions when both sides open streams.
/// The role is orthogonal to which side initiated the transport connection.
public enum MuxSessionRole: Sendable {
  case initiator
  case responder
}
