import std/asyncdispatch
import std/typedthreads

import threadtools

# Non-blocking sender tests for AsyncThreadQueueNotifier[T].
# These tests deliberately keep move-only payloads inside AsyncOwned[T]
# until explicitly extracted with take().

const
  BufSize = 64 * 1024

type
  Buf = object
    payload: seq[byte]
    tag: int
    expectedPtr: uint

  WorkerArgs = object
    tx: AsyncThreadQueueNotifier[Buf]
    count: int

proc dataPtr(buf: Buf): uint =
  if buf.payload.len == 0:
    return 0'u
  return cast[uint](unsafeAddr buf.payload[0])

proc makeBuf(tag: int): Buf =
  result.tag = tag
  result.payload = newSeq[byte](BufSize)
  for i in 0 ..< BufSize:
    result.payload[i] = byte((tag + i) mod 251)
  result.expectedPtr = dataPtr(result)

proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()

proc assertError[T](r: Result[T, ErrorCode]; expected: ErrorCode;
    msg: string) =
  doAssert r.isErr, msg & ": expected error"
  doAssert r.error == expected, msg & ": wrong error: " & $r.error

proc takeOk[T](box: AsyncOwned[T]; msg: string): T =
  var ret = box.take()
  doAssert ret.isOk, msg & ": " & $ret.error
  return ret.take()

proc checkBuf(buf: Buf; tag: int; ptrBefore: uint) =
  doAssert buf.tag == tag, "unexpected buffer tag"
  doAssert buf.payload.len == BufSize, "buffer size changed"
  doAssert dataPtr(buf) == ptrBefore, "buffer backing pointer changed"
  doAssert buf.payload[0] == byte(tag mod 251), "buffer contents changed"
  doAssert buf.payload[^1] == byte((tag + BufSize - 1) mod 251),
    "last byte changed"

proc testNoNotificationOnFullAndRetry() =
  var q = assertOk(newThreadQueue[Buf](1), "newThreadQueue")
  var bridge = assertOk(newAsyncThreadQueueBridge[Buf](q),
    "newAsyncThreadQueueBridge")
  let tx = bridge.notifier()

  # Register an async receive while the queue is empty.
  let pending = bridge.recvAsync()
  doAssert not pending.finished, "receive should initially be pending"

  # Deliberately bypass the notifier, filling the queue WITHOUT notifying
  # the bridge. This permits a pending Future and a full queue to coexist.
  var first = makeBuf(31)
  let firstPtr = dataPtr(first)
  doAssert assertOk(q.trySendMove(first), "prequeue without notification")
  doAssert not pending.finished, "prequeue did not notify the bridge"

  var second = makeBuf(47)
  let secondPtr = dataPtr(second)
  for attempt in 0 ..< 3:
    let full = tx.trySendMove(second)
    doAssert not assertOk(full, "notifier queue-full trySendMove"),
      "trySendMove must report full rather than waiting"
    checkBuf(second, 47, secondPtr)

  # If trySendMove() triggers AsyncEvent on failure, the pending receive will
  # incorrectly complete with 'first'. A dispatcher tick must not do that.
  waitFor sleepAsync(30)
  doAssert not pending.finished,
    "queue-full trySendMove incorrectly notified pending receiver"

  # Explicitly notify about the value deliberately enqueued without a signal.
  doAssert assertOk(tx.notify(), "explicit first notification")
  var firstReceived = takeOk(waitFor pending, "take first buffer")
  checkBuf(firstReceived, 31, firstPtr)

  # Now the queue has room. A successful trySendMove MUST notify a waiting
  # receiver, and the buffer must reach it without a copy.
  let nextPending = bridge.recvAsync()
  doAssert not nextPending.finished, "second receive should be pending"
  doAssert assertOk(tx.trySendMove(second), "successful retry")
  var secondReceived = takeOk(waitFor nextPending, "take retried buffer")
  checkBuf(secondReceived, 47, secondPtr)

  bridge.close()

proc worker(args: WorkerArgs) {.thread.} =
  doAssert args.tx.isValid, "worker notifier must be valid"
  for i in 0 ..< args.count:
    var data = makeBuf(i + 70)
    let sent = args.tx.trySendMove(data)
    doAssert sent.isOk, "worker trySendMove error: " & $sent.error
    doAssert sent.get(), "worker unexpectedly reached queue capacity"

proc testWorkerWakesPendingFutures() =
  const MsgCount = 3
  var q = assertOk(newThreadQueue[Buf](MsgCount + 1), "worker queue")
  var bridge = assertOk(newAsyncThreadQueueBridge[Buf](q), "worker bridge")
  let tx = bridge.notifier()

  # All futures are created before the thread sends its messages.
  let pending = [bridge.recvAsync(), bridge.recvAsync(), bridge.recvAsync()]
  for fut in pending:
    doAssert not fut.finished, "unexpected completion before worker starts"

  var th: Thread[WorkerArgs]
  createThread(th, worker, WorkerArgs(tx: tx, count: MsgCount))

  for i in 0 ..< MsgCount:
    var received = takeOk(waitFor pending[i], "worker pending receive")
    checkBuf(received, i + 70, received.expectedPtr)

  joinThread(th)
  bridge.close()

proc testMoveOnlyPoolItemOnFull() =
  var returnQ = assertOk(newThreadQueue[Buf](2), "pool item return queue")
  var itemQ = assertOk(newThreadQueue[PoolItem[Buf]](1),
    "pool item destination queue")
  var bridge = assertOk(newAsyncThreadQueueBridge[PoolItem[Buf]](itemQ),
    "pool item async bridge")
  let tx = bridge.notifier()

  var firstPayload = makeBuf(81)
  let firstPtr = dataPtr(firstPayload)
  var first = newPoolItem[Buf](returnQ, firstPayload)
  doAssert assertOk(tx.trySendMove(first), "first PoolItem send")

  var secondPayload = makeBuf(93)
  let secondPtr = dataPtr(secondPayload)
  var second = newPoolItem[Buf](returnQ, secondPayload)

  for attempt in 0 ..< 3:
    let full = tx.trySendMove(second)
    doAssert not assertOk(full, "full PoolItem send")
    doAssert second.isActive, "failed send deactivated PoolItem"
    checkBuf(second.item, 93, secondPtr)

    # Failed transfers must not invoke PoolItem's auto-return destructor.
    var returned: Buf
    let empty = returnQ.tryReceive(returned)
    doAssert not assertOk(empty, "check pool return queue"),
      "failed send returned an active PoolItem to its source pool"

  var receivedFirst = takeOk(waitFor bridge.recvAsync(),
    "take first PoolItem")
  doAssert receivedFirst.isActive
  checkBuf(receivedFirst.item, 81, firstPtr)

  let nextPending = bridge.recvAsync()
  doAssert assertOk(tx.trySendMove(second), "retry PoolItem send")
  var receivedSecond = takeOk(waitFor nextPending,
    "take retried PoolItem")
  doAssert receivedSecond.isActive
  checkBuf(receivedSecond.item, 93, secondPtr)

  doAssert assertOk(receivedFirst.release(), "release first PoolItem")
  doAssert assertOk(receivedSecond.release(), "release second PoolItem")

  var returnedFirst = returnQ.receive()
  var returnedSecond = returnQ.receive()
  checkBuf(returnedFirst, 81, firstPtr)
  checkBuf(returnedSecond, 93, secondPtr)

  bridge.close()

proc testInvalidAndClosedKeepOwnership() =
  var invalid: AsyncThreadQueueNotifier[Buf]
  var data = makeBuf(101)
  let originalPtr = dataPtr(data)

  assertError(invalid.trySendMove(data), ErrorCode.InvalidState,
    "uninitialized notifier")
  checkBuf(data, 101, originalPtr)

  var q = assertOk(newThreadQueue[Buf](1), "closed test queue")
  var bridge = assertOk(newAsyncThreadQueueBridge[Buf](q),
    "closed test bridge")
  let tx = bridge.notifier()
  bridge.close()

  assertError(tx.trySendMove(data), ErrorCode.Closed, "closed bridge")
  checkBuf(data, 101, originalPtr)

  var q2 = assertOk(newThreadQueue[Buf](1), "closed queue test")
  var bridge2 = assertOk(newAsyncThreadQueueBridge[Buf](q2),
    "closed queue bridge")
  let tx2 = bridge2.notifier()
  q2.close()

  assertError(tx2.trySendMove(data), ErrorCode.Closed,
    "closed underlying queue")
  checkBuf(data, 101, originalPtr)
  bridge2.close()

proc main() =
  testNoNotificationOnFullAndRetry()
  testWorkerWakesPendingFutures()
  testMoveOnlyPoolItemOnFull()
  testInvalidAndClosedKeepOwnership()
  echo "OK: async bridge non-blocking send tests passed"

main()
