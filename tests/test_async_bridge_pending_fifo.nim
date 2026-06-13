import std/asyncdispatch
import std/typedthreads

import threadtools

# This test covers the case where multiple recvAsync() calls are already
# pending before worker threads enqueue values.
#
# It also covers values that were queued before recvAsync() is called.
#
# The queue capacity is intentionally larger than MsgCount here.  This test is
# about async bridge FIFO behavior and pointer stability, not backpressure.
# A smaller capacity would make the prequeue test block before any receive is
# started.

const
  MsgCount = 512
  BufSize = 2048
  QueueCapacity = MsgCount + 16

type
  Msg = object
    data: seq[byte]
    expectedPtr: uint
    index: int
    seed: int

  WorkerArgs = object
    tx: AsyncThreadQueueNotifier[Msg]
    count: int
    size: int

proc dataPtr(data: seq[byte]): uint =
  if data.len == 0:
    return 0'u
  return cast[uint](unsafeAddr data[0])

proc dataPtr(msg: Msg): uint =
  return dataPtr(msg.data)

proc makeMsg(index: int; size: int): Msg =
  result.data = newSeq[byte](size)
  result.index = index
  result.seed = (index * 31 + 7) mod 251

  for i in 0 ..< size:
    result.data[i] = byte((result.seed + i) mod 251)

  result.expectedPtr = dataPtr(result)

proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()

proc takeOk[T](box: AsyncOwned[T]; msg: string): T =
  var ret = box.take()
  doAssert ret.isOk, msg & ": " & $ret.error
  return ret.take()

proc worker(args: WorkerArgs) {.thread.} =
  doAssert args.tx.isValid, "worker notifier should be valid"

  for i in 0 ..< args.count:
    var msg = makeMsg(i, args.size)
    let ret = args.tx.sendMove(msg)
    doAssert ret.isOk, "worker sendMove failed: " & $ret.error

proc testPendingReceivesBeforeWorkerSends() =
  var q = assertOk(newThreadQueue[Msg](QueueCapacity), "newThreadQueue[Msg] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Msg](q), "newAsyncThreadQueueBridge[Msg] failed")
  let tx = bridge.notifier()

  var futures: seq[Future[AsyncOwned[Msg]]] = @[]
  futures.setLen(MsgCount)

  # Queue all receives first.  No values are available yet, so these futures
  # should remain pending until the worker thread sends values and triggers the
  # AsyncEvent.
  for i in 0 ..< MsgCount:
    futures[i] = bridge.recvAsync()
    doAssert not futures[i].finished, "recvAsync future finished before any send"

  var th: Thread[WorkerArgs]
  createThread(th, worker, WorkerArgs(
    tx: tx,
    count: MsgCount,
    size: BufSize,
  ))

  for i in 0 ..< MsgCount:
    let box = waitFor futures[i]
    var msg = takeOk(box, "AsyncOwned[Msg].take failed")

    # Both ThreadQueue and bridge pending list are FIFO.  Since all futures were
    # registered before worker sends, future[i] should receive message i.
    doAssert msg.index == i, "pending future completed out of FIFO order"
    doAssert dataPtr(msg) == msg.expectedPtr, "pending FIFO bridge changed seq backing pointer"
    doAssert msg.data.len == BufSize, "pending FIFO bridge returned wrong length"
    doAssert msg.data[0] == byte(msg.seed), "pending FIFO bridge returned wrong payload"

  joinThread(th)
  bridge.close()

proc testPrequeuedValuesDrainInFifoOrder() =
  var q = assertOk(newThreadQueue[Msg](QueueCapacity), "newThreadQueue[Msg] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Msg](q), "newAsyncThreadQueueBridge[Msg] failed")
  let tx = bridge.notifier()

  # Send values before recvAsync().  The capacity is larger than MsgCount so
  # this does not test blocking/backpressure; it only tests FIFO drain of
  # already queued values.
  for i in 0 ..< MsgCount:
    var msg = makeMsg(i, BufSize)
    let ret = tx.sendMove(msg)
    doAssert ret.isOk, "prequeue sendMove failed: " & $ret.error

  for i in 0 ..< MsgCount:
    let box = waitFor bridge.recvAsync()
    var msg = takeOk(box, "AsyncOwned[Msg].take failed for prequeued value")

    doAssert msg.index == i, "prequeued value drained out of FIFO order"
    doAssert dataPtr(msg) == msg.expectedPtr, "prequeued drain changed seq backing pointer"
    doAssert msg.data[0] == byte(msg.seed), "prequeued drain returned wrong payload"

  bridge.close()

proc main() =
  testPendingReceivesBeforeWorkerSends()
  testPrequeuedValuesDrainInFifoOrder()

main()
echo "OK: async bridge pending FIFO tests passed"
