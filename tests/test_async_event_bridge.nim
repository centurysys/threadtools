import std/asyncdispatch
import threadtools

# This test intentionally avoids async proc for move-only payloads.  async macro
# locals are lifted into an environment object, and moving fields out of that
# object can introduce implicit-copy requirements.

type
  Buf = object
    data: seq[byte]

proc makeBuf(n: int; seed: int): Buf =
  result.data = newSeq[byte](n)
  for i in 0 ..< n:
    result.data[i] = byte((i + seed) mod 251)

proc dataPtr(buf: Buf): uint =
  if buf.data.len == 0:
    return 0'u
  return cast[uint](unsafeAddr buf.data[0])

proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()

proc takeOk[T](box: AsyncOwned[T]; msg: string): T =
  var ret = box.take()
  doAssert ret.isOk, msg & ": " & $ret.error
  return ret.take()

proc testEventValueTransfer() =
  var q = assertOk(newThreadQueue[Buf](8), "newThreadQueue[Buf] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Buf](q), "newAsyncThreadQueueBridge[Buf] failed")
  let tx = bridge.notifier()

  var buf = makeBuf(64 * 1024, 31)
  let before = dataPtr(buf)

  let pending = bridge.recvAsync()

  let sent = tx.sendMove(buf)
  doAssert sent.isOk, "AsyncThreadQueueNotifier.sendMove failed: " & $sent.error

  let box = waitFor pending
  doAssert box.isActive, "event bridge returned inactive AsyncOwned[Buf]"

  var received = takeOk(box, "AsyncOwned[Buf].take failed")
  doAssert not box.isActive, "AsyncOwned[Buf] should be inactive after take"
  doAssert dataPtr(received) == before, "event bridge changed seq backing pointer"
  doAssert received.data[0] == byte(31), "event bridge returned wrong payload"

  bridge.close()

proc testEventPrequeuedValue() =
  var q = assertOk(newThreadQueue[Buf](8), "newThreadQueue[Buf] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Buf](q), "newAsyncThreadQueueBridge[Buf] failed")
  let tx = bridge.notifier()

  var buf = makeBuf(64 * 1024, 41)
  let before = dataPtr(buf)

  let sent = tx.sendMove(buf)
  doAssert sent.isOk, "prequeue sendMove failed: " & $sent.error

  # The value is already in the queue.  recvAsync() should complete without
  # relying on a later trigger.
  let box = waitFor bridge.recvAsync()
  var received = takeOk(box, "prequeued AsyncOwned[Buf].take failed")
  doAssert dataPtr(received) == before, "prequeued event bridge changed seq backing pointer"
  doAssert received.data[0] == byte(41), "prequeued event bridge returned wrong payload"

  bridge.close()

proc testEventPoolItemTransfer() =
  var returnQ = assertOk(newThreadQueue[Buf](8), "newThreadQueue returnQ failed")
  var itemQ = assertOk(newThreadQueue[PoolItem[Buf]](8), "newThreadQueue itemQ failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[PoolItem[Buf]](itemQ), "newAsyncThreadQueueBridge[PoolItem[Buf]] failed")
  let tx = bridge.notifier()

  var buf = makeBuf(64 * 1024, 51)
  let before = dataPtr(buf)

  var item = newPoolItem[Buf](returnQ, buf)
  doAssert item.isActive, "new PoolItem should be active"
  doAssert dataPtr(item.item) == before, "newPoolItem changed seq backing pointer"

  let pending = bridge.recvAsync()

  let sent = tx.sendMove(item)
  doAssert sent.isOk, "AsyncThreadQueueNotifier.sendMove(PoolItem) failed: " & $sent.error

  let box = waitFor pending
  doAssert box.isActive, "event bridge returned inactive AsyncOwned[PoolItem[Buf]]"

  var receivedItem = takeOk(box, "AsyncOwned[PoolItem[Buf]].take failed")
  doAssert not box.isActive, "AsyncOwned[PoolItem[Buf]] should be inactive after take"
  doAssert receivedItem.isActive, "event bridge returned inactive PoolItem"
  doAssert dataPtr(receivedItem.item) == before, "event bridge PoolItem changed seq backing pointer"
  doAssert receivedItem.item.data[0] == byte(51), "event bridge PoolItem returned wrong payload"

  let released = receivedItem.release()
  doAssert released.isOk, "received PoolItem.release failed: " & $released.error

  var returned = returnQ.receive()
  doAssert dataPtr(returned) == before, "PoolItem event release changed seq backing pointer"
  doAssert returned.data[0] == byte(51), "returned payload mismatch after event PoolItem release"

  bridge.close()

proc main() =
  testEventValueTransfer()
  testEventPrequeuedValue()
  testEventPoolItemTransfer()

main()
echo "OK: async event bridge tests passed"
