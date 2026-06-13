import std/asyncdispatch
import std/os
import std/typedthreads

import threadtools

# This test covers bounded-queue backpressure across the async bridge.
#
# The worker thread sends more messages than the queue capacity.
# It should block when the bounded queue becomes full.
#
# The dispatcher/main thread then receives values through recvAsync().
# As recvAsync() drains the queue, the worker thread can continue sending.
#
# This test intentionally keeps the queue capacity small so that the worker
# must hit backpressure.  It also uses a separate report queue to observe how
# many sends completed before the receiver started draining.

const
  QueueCapacity = 4
  MsgCount = 64
  BufSize = 1024

type
  Msg = object
    data: seq[byte]
    expectedPtr: uint
    index: int
    seed: int

  WorkerArgs = object
    tx: AsyncThreadQueueNotifier[Msg]
    reportQ: ThreadQueueHandle[int]
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
  result.seed = (index * 37 + 11) mod 251

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

proc drainReports(reportQ: var ThreadQueue[int]): int =
  var count = 0

  while true:
    var value: int
    let ret = reportQ.tryReceive(value)
    doAssert ret.isOk, "reportQ.tryReceive failed: " & $ret.error

    if not ret.get():
      break

    inc count

  return count

proc worker(args: WorkerArgs) {.thread.} =
  doAssert args.tx.isValid, "worker notifier should be valid"
  doAssert args.reportQ != nil, "worker reportQ should not be nil"

  for i in 0 ..< args.count:
    var msg = makeMsg(i, args.size)
    let sendRet = args.tx.sendMove(msg)
    doAssert sendRet.isOk, "worker sendMove failed: " & $sendRet.error

    # Report that a send completed.  The report queue is intentionally larger
    # than MsgCount, so it should not be the source of backpressure.
    var report = i
    let reportRet = args.reportQ.sendMove(report)
    doAssert reportRet.isOk, "worker report sendMove failed: " & $reportRet.error

proc testWorkerBlocksUntilDispatcherDrains() =
  var q = assertOk(newThreadQueue[Msg](QueueCapacity), "newThreadQueue[Msg] failed")
  var reportQ = assertOk(newThreadQueue[int](MsgCount + 16), "newThreadQueue[int] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Msg](q), "newAsyncThreadQueueBridge[Msg] failed")
  let tx = bridge.notifier()

  var th: Thread[WorkerArgs]
  createThread(th, worker, WorkerArgs(
    tx: tx,
    reportQ: reportQ.handle,
    count: MsgCount,
    size: BufSize,
  ))

  # Give the worker a chance to fill the bounded queue before the receiver
  # starts draining it.  Since the queue capacity is only 4, the worker should
  # not be able to complete all sends at this point.
  var completedBeforeDrain = 0
  for _ in 0 ..< 100:
    sleep(10)
    completedBeforeDrain += drainReports(reportQ)
    if completedBeforeDrain > 0:
      break

  doAssert completedBeforeDrain > 0, "worker did not complete even the first send before drain"
  doAssert completedBeforeDrain <= QueueCapacity,
    "worker completed more sends than queue capacity before receiver drained"

  var totalReports = completedBeforeDrain

  for i in 0 ..< MsgCount:
    let box = waitFor bridge.recvAsync()
    var msg = takeOk(box, "AsyncOwned[Msg].take failed")

    doAssert msg.index == i, "backpressure bridge delivered messages out of FIFO order"
    doAssert dataPtr(msg) == msg.expectedPtr, "backpressure bridge changed seq backing pointer"
    doAssert msg.data.len == BufSize, "backpressure bridge returned wrong data length"
    doAssert msg.data[0] == byte(msg.seed), "backpressure bridge returned wrong payload"

    totalReports += drainReports(reportQ)

  joinThread(th)

  totalReports += drainReports(reportQ)
  doAssert totalReports == MsgCount, "worker did not report all completed sends"

  bridge.close()

proc main() =
  testWorkerBlocksUntilDispatcherDrains()

main()
echo "OK: async bridge backpressure tests passed"
