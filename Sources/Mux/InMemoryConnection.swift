#if canImport(Glibc)
  import Glibc
#elseif canImport(Musl)
  import Musl
#elseif canImport(Darwin)
  import Darwin
#endif

/// Paired in-memory connections for testing.
///
/// Two `InMemoryConnection` instances are created together and cross-wired:
/// writes to one appear as reads on the other. No I/O, no threads, fully
/// deterministic.
public final class InMemoryConnection: Connection, @unchecked Sendable {
  private let readPipe: _PipeState
  private let writePipe: _PipeState

  private init(readPipe: _PipeState, writePipe: _PipeState) {
    self.readPipe = readPipe
    self.writePipe = writePipe
  }

  /// Create a pair of connected in-memory connections.
  /// Writes to the first appear as reads on the second, and vice versa.
  public static func makePair() -> (InMemoryConnection, InMemoryConnection) {
    let pipe1 = _PipeState()
    let pipe2 = _PipeState()
    let a = InMemoryConnection(readPipe: pipe1, writePipe: pipe2)
    let b = InMemoryConnection(readPipe: pipe2, writePipe: pipe1)
    return (a, b)
  }

  public func read(into buffer: UnsafeMutableRawBufferPointer) async throws -> Int {
    try await readPipe.read(into: buffer)
  }

  public func write(contentsOf bytes: [UInt8]) async throws {
    try await writePipe.write(bytes)
  }

  public func close() async {
    readPipe.signalEOF()
    writePipe.signalEOF()
  }
}

// MARK: - Internal pipe state

/// A unidirectional byte pipe. One side writes, the other reads.
/// Thread-safe via pthread mutex + continuations.
final class _PipeState: @unchecked Sendable {
  private var buffer: [UInt8] = []
  private var eof = false
  private var waitingReader: CheckedContinuation<Int, any Error>?
  private var waitingReaderBuffer: UnsafeMutableRawBufferPointer?
  private var _lock = pthread_mutex_t()

  init() {
    pthread_mutex_init(&_lock, nil)
  }

  deinit {
    pthread_mutex_destroy(&_lock)
  }

  private func lock() {
    pthread_mutex_lock(&_lock)
  }

  private func unlock() {
    pthread_mutex_unlock(&_lock)
  }

  func write(_ bytes: [UInt8]) async throws {
    guard !bytes.isEmpty else { return }

    lock()
    if eof {
      unlock()
      throw MuxError.connectionLost
    }

    // If there's a waiting reader, deliver directly
    if let reader = waitingReader, let readerBuf = waitingReaderBuffer {
      waitingReader = nil
      waitingReaderBuffer = nil
      let count = min(bytes.count, readerBuf.count)
      bytes.withUnsafeBytes { src in
        readerBuf.copyBytes(from: src.prefix(count))
      }
      if count < bytes.count {
        buffer.append(contentsOf: bytes[count...])
      }
      unlock()
      reader.resume(returning: count)
      return
    }

    // No reader waiting — buffer it
    buffer.append(contentsOf: bytes)
    unlock()
  }

  func read(into target: UnsafeMutableRawBufferPointer) async throws -> Int {
    lock()

    // Data available — return immediately
    if !buffer.isEmpty {
      let count = min(buffer.count, target.count)
      buffer.withUnsafeBytes { src in
        target.copyBytes(from: src.prefix(count))
      }
      buffer.removeFirst(count)
      unlock()
      return count
    }

    // EOF
    if eof {
      unlock()
      return 0
    }

    // Need to wait
    unlock()
    return try await withCheckedThrowingContinuation { cont in
      lock()

      // Double-check after re-acquiring lock
      if !buffer.isEmpty {
        let count = min(buffer.count, target.count)
        buffer.withUnsafeBytes { src in
          target.copyBytes(from: src.prefix(count))
        }
        buffer.removeFirst(count)
        unlock()
        cont.resume(returning: count)
        return
      }
      if eof {
        unlock()
        cont.resume(returning: 0)
        return
      }

      waitingReader = cont
      waitingReaderBuffer = target
      unlock()
    }
  }

  func signalEOF() {
    lock()
    eof = true
    if let reader = waitingReader {
      waitingReader = nil
      waitingReaderBuffer = nil
      unlock()
      reader.resume(returning: 0)
      return
    }
    unlock()
  }
}
