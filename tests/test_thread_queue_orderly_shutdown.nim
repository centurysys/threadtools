import std/os
import std/typedthreads
import threadtools

# A dedicated consumer models a synchronous SQLite writer thread. The producer
# stops publishing, enqueues a final sentinel, drains via the consumer, then
# joins the consumer BEFORE closing the queue. close() is not a wake-up signal.
const
  MessageCount = 200
  MessageSize = 2048
  MaxRetries = 10_000

type
  MessageKind = enum
    mkRecord, mkStop

  Message = object
    kind: MessageKind
    sequence: int
    expectedPtr: uint
    bytes: seq[byte]

  Summary = object
    count: int
    sequenceSum: int

  WriterArgs = object
    input: ThreadQueue[Message]
    summary: ThreadQueue[Summary]

proc assertOk[T](value: Result[T, ErrorCode]; context: string): T =
  doAssert value.isOk, context & ": " & $value.error
  return value.get()

proc dataPtr(message: Message): uint =
  if message.bytes.len == 0:
    return 0'u
  return cast[uint](unsafeAddr message.bytes[0])

proc makeMessage(sequence: int): Message =
  result.kind = mkRecord
  result.sequence = sequence
  result.bytes = newSeq[byte](MessageSize)
  for i in 0 ..< MessageSize:
    result.bytes[i] = byte((sequence + i) mod 251)
  result.expectedPtr = dataPtr(result)

proc writerMain(args: WriterArgs) {.thread.} =
  var summary = Summary()
  while true:
    var message = args.input.receive()
    if message.kind == mkStop:
      break

    doAssert message.kind == mkRecord
    doAssert message.sequence == summary.count,
      "writer observed out-of-order message"
    doAssert message.bytes.len == MessageSize
    doAssert dataPtr(message) == message.expectedPtr,
      "record buffer was copied"
    doAssert message.bytes[0] == byte(message.sequence mod 251)
    doAssert message.bytes[^1] == byte((message.sequence + MessageSize - 1) mod 251)
    inc summary.count
    summary.sequenceSum += message.sequence
    if summary.count mod 8 == 0:
      sleep(1) # emulate occasional SQLite transaction latency

  let sent = args.summary.sendMove(summary)
  doAssert sent.isOk, "writer summary send failed: " & $sent.error

proc enqueueWithRetry(queue: ThreadQueue[Message]; message: var Message) =
  var attempts = 0
  while true:
    let sent = queue.trySendMove(message)
    doAssert sent.isOk, "queue send failed: " & $sent.error
    if sent.get():
      return
    inc attempts
    doAssert attempts < MaxRetries, "writer failed to drain queue"
    sleep(1)

proc testGracefulDrainAndClose() =
  var input = assertOk(newThreadQueue[Message](2), "input queue")
  var summaryQ = assertOk(newThreadQueue[Summary](1), "summary queue")
  var writer: Thread[WriterArgs]
  createThread(writer, writerMain,
    WriterArgs(input: input, summary: summaryQ))

  for i in 0 ..< MessageCount:
    var message = makeMessage(i)
    enqueueWithRetry(input, message)

  # No more producers are permitted after this point. A sentinel travels
  # behind every preceding record because this test has a single producer.
  var stop = Message(kind: mkStop)
  enqueueWithRetry(input, stop)

  # Writer sends the summary and exits only after receiving the sentinel.
  joinThread(writer)
  let summary = summaryQ.receive()
  doAssert summary.count == MessageCount, "shutdown lost records"
  doAssert summary.sequenceSum == MessageCount * (MessageCount - 1) div 2,
    "shutdown record checksum mismatch"

  var unexpected: Message
  let remaining = input.tryReceive(unexpected)
  doAssert remaining.isOk and not remaining.get(), "queue was not drained"

  # This close is after join: concurrent close/send/receive is unsupported.
  input.close()
  summaryQ.close()

  var afterClose = makeMessage(42)
  let original = dataPtr(afterClose)
  let rejected = input.trySendMove(afterClose)
  doAssert rejected.isErr and rejected.error == ErrorCode.Closed
  doAssert dataPtr(afterClose) == original,
    "closed queue consumed the caller's buffer"
  doAssert afterClose.bytes.len == MessageSize

when isMainModule:
  testGracefulDrainAndClose()
  echo "OK: queue orderly shutdown and drain tests passed"
