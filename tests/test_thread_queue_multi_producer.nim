import std/os
import std/typedthreads
import threadtools

# A small-capacity queue forces competing producers to retry. Verify ownership
# after every failed send, and exact-once delivery plus FIFO per producer.
const
  ProducerCount = 4
  PacketsPerProducer = 100
  PacketSize = 4096
  MaxIdleCycles = 10_000

type
  Packet = object
    producer: int
    sequence: int
    expectedPtr: uint
    bytes: seq[byte]

  ProducerArgs = object
    queue: ThreadQueue[Packet]
    producer: int

proc assertOk[T](value: Result[T, ErrorCode]; context: string): T =
  doAssert value.isOk, context & ": " & $value.error
  return value.get()

proc dataPtr(packet: Packet): uint =
  if packet.bytes.len == 0:
    return 0'u
  return cast[uint](unsafeAddr packet.bytes[0])

proc makePacket(producer, sequence: int): Packet =
  result.producer = producer
  result.sequence = sequence
  result.bytes = newSeq[byte](PacketSize)
  let seed = (producer * 37 + sequence) mod 251
  for i in 0 ..< PacketSize:
    result.bytes[i] = byte((seed + i) mod 251)
  result.expectedPtr = dataPtr(result)

proc verifyPacket(packet: Packet) =
  doAssert packet.producer in 0 ..< ProducerCount
  doAssert packet.sequence in 0 ..< PacketsPerProducer
  doAssert packet.bytes.len == PacketSize
  doAssert dataPtr(packet) == packet.expectedPtr,
    "backing pointer changed during ownership transfer"
  let seed = (packet.producer * 37 + packet.sequence) mod 251
  doAssert packet.bytes[0] == byte(seed)
  doAssert packet.bytes[^1] == byte((seed + PacketSize - 1) mod 251)

proc producerMain(args: ProducerArgs) {.thread.} =
  for sequence in 0 ..< PacketsPerProducer:
    var packet = makePacket(args.producer, sequence)
    let expectedPtr = dataPtr(packet)
    var attempts = 0
    while true:
      let sent = args.queue.trySendMove(packet)
      doAssert sent.isOk, "producer trySendMove failed: " & $sent.error
      if sent.get():
        break
      # Queue full: ownership, content and pointer must still belong here.
      doAssert dataPtr(packet) == expectedPtr
      verifyPacket(packet)
      inc attempts
      doAssert attempts < MaxIdleCycles, "producer stalled on a full queue"
      sleep(1)

proc testFullQueueOwnership() =
  var q = assertOk(newThreadQueue[Packet](1), "newThreadQueue")
  var first = makePacket(0, 0)
  doAssert assertOk(q.trySendMove(first), "first trySendMove")

  var second = makePacket(1, 0)
  let before = dataPtr(second)
  for _ in 0 ..< 5:
    let ret = q.trySendMove(second)
    doAssert ret.isOk and not ret.get(), "expected queue-full result"
    doAssert dataPtr(second) == before
    verifyPacket(second)

  var received = q.receive()
  verifyPacket(received)
  doAssert assertOk(q.trySendMove(second), "retry trySendMove")
  received = q.receive()
  verifyPacket(received)
  doAssert dataPtr(received) == before
  q.close()

proc testConcurrentProducers() =
  const Total = ProducerCount * PacketsPerProducer
  var q = assertOk(newThreadQueue[Packet](2), "newThreadQueue")
  var workers: array[ProducerCount, Thread[ProducerArgs]]
  for producer in 0 ..< ProducerCount:
    createThread(workers[producer], producerMain,
      ProducerArgs(queue: q, producer: producer))

  var seen = newSeq[bool](Total)
  var lastSequence: array[ProducerCount, int]
  for p in 0 ..< ProducerCount:
    lastSequence[p] = -1

  var count = 0
  var idleCycles = 0
  while count < Total:
    var packet: Packet
    let received = q.tryReceive(packet)
    doAssert received.isOk, "tryReceive failed: " & $received.error
    if not received.get():
      inc idleCycles
      doAssert idleCycles < MaxIdleCycles, "receiver stalled"
      sleep(1)
      continue

    idleCycles = 0
    verifyPacket(packet)
    let key = packet.producer * PacketsPerProducer + packet.sequence
    doAssert not seen[key], "duplicate packet"
    doAssert packet.sequence == lastSequence[packet.producer] + 1,
      "producer FIFO violated"
    seen[key] = true
    lastSequence[packet.producer] = packet.sequence
    inc count
    if count mod 7 == 0:
      sleep(1) # make producer-side queue-full retries more likely

  for producer in 0 ..< ProducerCount:
    joinThread(workers[producer])

  for found in seen:
    doAssert found, "missing packet"
  var unexpected: Packet
  let extra = q.tryReceive(unexpected)
  doAssert extra.isOk and not extra.get(), "unexpected queued packet"
  q.close()

when isMainModule:
  testFullQueueOwnership()
  testConcurrentProducers()
  echo "OK: multi-producer trySendMove stress tests passed"
