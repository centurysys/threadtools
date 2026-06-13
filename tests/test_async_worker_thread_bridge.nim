import std/asyncdispatch
import std/typedthreads

import threadtools

# This test exercises the real intended direction:
#
#   worker thread:
#     AsyncThreadQueueNotifier[T].sendMove(value)
#
#   dispatcher/main thread:
#     waitFor bridge.recvAsync()
#     AsyncOwned[T].take()
#
# The test intentionally avoids async proc for move-only payloads.  async macro
# locals are lifted into an environment object, and moving fields out of that
# object can introduce implicit-copy requirements.

const
  MsgCount = 1000
  BufSize = 4096

type
  Buf = object
    data: seq[byte]
    expectedPtr: uint
    index: int
    seed: int

  Msg = object
    data: seq[byte]
    expectedPtr: uint
    index: int
    seed: int

  ValueWorkerArgs = object
    tx: AsyncThreadQueueNotifier[Msg]
    count: int
    size: int

  PoolItemWorkerArgs = object
    tx: AsyncThreadQueueNotifier[PoolItem[Buf]]
    returnQ: ThreadQueueHandle[Buf]
    count: int
    size: int

proc dataPtr(data: seq[byte]): uint =
  if data.len == 0:
    return 0'u
  return cast[uint](unsafeAddr data[0])

proc dataPtr(msg: Msg): uint =
  return dataPtr(msg.data)

proc dataPtr(buf: Buf): uint =
  return dataPtr(buf.data)

proc makeMsg(index: int; size: int): Msg =
  result.data = newSeq[byte](size)
  result.index = index
  result.seed = (index * 17 + 13) mod 251

  for i in 0 ..< size:
    result.data[i] = byte((result.seed + i) mod 251)

  result.expectedPtr = dataPtr(result)

proc makeBuf(index: int; size: int): Buf =
  result.data = newSeq[byte](size)
  result.index = index
  result.seed = (index * 19 + 23) mod 251

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

proc valueWorker(args: ValueWorkerArgs) {.thread.} =
  doAssert args.tx.isValid, "value worker notifier should be valid"

  for i in 0 ..< args.count:
    var msg = makeMsg(i, args.size)
    let ret = args.tx.sendMove(msg)
    doAssert ret.isOk, "value worker sendMove failed: " & $ret.error

proc poolItemWorker(args: PoolItemWorkerArgs) {.thread.} =
  doAssert args.tx.isValid, "pool item worker notifier should be valid"
  doAssert args.returnQ != nil, "pool item worker returnQ should not be nil"

  for i in 0 ..< args.count:
    var buf = makeBuf(i, args.size)
    var item = newPoolItem[Buf](args.returnQ, move buf)

    let ret = args.tx.sendMove(item)
    doAssert ret.isOk, "pool item worker sendMove failed: " & $ret.error

proc testWorkerThreadValueTransfer() =
  var q = assertOk(newThreadQueue[Msg](128), "newThreadQueue[Msg] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Msg](q), "newAsyncThreadQueueBridge[Msg] failed")
  let tx = bridge.notifier()

  var th: Thread[ValueWorkerArgs]
  createThread(th, valueWorker, ValueWorkerArgs(
    tx: tx,
    count: MsgCount,
    size: BufSize,
  ))

  var seen = newSeq[bool](MsgCount)
  var sumIndex = 0

  for _ in 0 ..< MsgCount:
    let box = waitFor bridge.recvAsync()
    var msg = takeOk(box, "AsyncOwned[Msg].take failed")

    doAssert msg.index >= 0 and msg.index < MsgCount, "received index out of range"
    doAssert not seen[msg.index], "received duplicate index"
    seen[msg.index] = true
    sumIndex += msg.index

    doAssert dataPtr(msg) == msg.expectedPtr, "worker value transfer changed seq backing pointer"
    doAssert msg.data.len == BufSize, "worker value transfer returned wrong data length"
    doAssert msg.data[0] == byte(msg.seed), "worker value transfer returned wrong payload"

  joinThread(th)

  let expectedSum = (MsgCount - 1) * MsgCount div 2
  doAssert sumIndex == expectedSum, "worker value transfer missed messages"

  bridge.close()

proc testWorkerThreadPoolItemTransfer() =
  var returnQ = assertOk(newThreadQueue[Buf](128), "newThreadQueue returnQ failed")
  var itemQ = assertOk(newThreadQueue[PoolItem[Buf]](128), "newThreadQueue itemQ failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[PoolItem[Buf]](itemQ), "newAsyncThreadQueueBridge[PoolItem[Buf]] failed")
  let tx = bridge.notifier()

  var th: Thread[PoolItemWorkerArgs]
  createThread(th, poolItemWorker, PoolItemWorkerArgs(
    tx: tx,
    returnQ: returnQ.handle,
    count: MsgCount,
    size: BufSize,
  ))

  var seen = newSeq[bool](MsgCount)
  var sumIndex = 0

  for _ in 0 ..< MsgCount:
    let box = waitFor bridge.recvAsync()
    var item = takeOk(box, "AsyncOwned[PoolItem[Buf]].take failed")

    doAssert item.isActive, "received PoolItem should be active"

    let index = item.item.index
    doAssert index >= 0 and index < MsgCount, "received PoolItem index out of range"
    doAssert not seen[index], "received duplicate PoolItem index"
    seen[index] = true
    sumIndex += index

    doAssert dataPtr(item.item) == item.item.expectedPtr, "worker PoolItem transfer changed seq backing pointer"
    doAssert item.item.data.len == BufSize, "worker PoolItem transfer returned wrong data length"
    doAssert item.item.data[0] == byte(item.item.seed), "worker PoolItem transfer returned wrong payload"

    let released = item.release()
    doAssert released.isOk, "received PoolItem.release failed: " & $released.error

    var returned = returnQ.receive()
    doAssert returned.index == index, "PoolItem returned wrong buffer index"
    doAssert dataPtr(returned) == returned.expectedPtr, "PoolItem return changed seq backing pointer"
    doAssert returned.data[0] == byte(returned.seed), "PoolItem returned wrong buffer payload"

  joinThread(th)

  let expectedSum = (MsgCount - 1) * MsgCount div 2
  doAssert sumIndex == expectedSum, "worker PoolItem transfer missed messages"

  bridge.close()

proc main() =
  testWorkerThreadValueTransfer()
  testWorkerThreadPoolItemTransfer()

main()
echo "OK: async worker thread bridge tests passed"
