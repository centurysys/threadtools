import std/asyncdispatch
import threadtools

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

proc testAsyncValueTransfer() =
  var q = assertOk(newThreadQueue[Buf](8), "newThreadQueue[Buf] failed")

  var buf = makeBuf(64 * 1024, 11)
  let before = dataPtr(buf)

  let pending = q.recvAsync(1)

  # recvAsync schedules polling with callSoon().  Send after creating the Future
  # so the value is received by the polling callback path.
  let sent = q.sendMove(buf)
  doAssert sent.isOk, "sendMove before waitFor failed: " & $sent.error

  let box = waitFor pending
  doAssert box.isActive, "recvAsync returned inactive AsyncOwned[Buf]"

  var received = takeOk(box, "AsyncOwned[Buf].take failed")
  doAssert not box.isActive, "AsyncOwned[Buf] should be inactive after take"
  doAssert dataPtr(received) == before, "recvAsync changed seq backing pointer"
  doAssert received.data[0] == byte(11), "recvAsync returned wrong payload"

proc testAsyncPoolItemTransfer() =
  var returnQ = assertOk(newThreadQueue[Buf](8), "newThreadQueue returnQ failed")
  var itemQ = assertOk(newThreadQueue[PoolItem[Buf]](8), "newThreadQueue itemQ failed")

  var buf = makeBuf(64 * 1024, 22)
  let before = dataPtr(buf)

  var item = newPoolItem[Buf](returnQ, buf)
  doAssert item.isActive, "new PoolItem should be active"
  doAssert dataPtr(item.item) == before, "newPoolItem changed seq backing pointer"

  let pending = itemQ.recvAsync(1)

  let sent = itemQ.sendMove(item)
  doAssert sent.isOk, "sendMove(PoolItem) before waitFor failed: " & $sent.error

  let box = waitFor pending
  doAssert box.isActive, "recvAsync returned inactive AsyncOwned[PoolItem[Buf]]"

  var receivedItem = takeOk(box, "AsyncOwned[PoolItem[Buf]].take failed")
  doAssert not box.isActive, "AsyncOwned[PoolItem[Buf]] should be inactive after take"
  doAssert receivedItem.isActive, "recvAsync returned inactive PoolItem"
  doAssert dataPtr(receivedItem.item) == before, "recvAsync(PoolItem) changed seq backing pointer"
  doAssert receivedItem.item.data[0] == byte(22), "recvAsync(PoolItem) returned wrong payload"

  let released = receivedItem.release()
  doAssert released.isOk, "received PoolItem.release failed: " & $released.error

  var returned = returnQ.receive()
  doAssert dataPtr(returned) == before, "PoolItem async release changed seq backing pointer"
  doAssert returned.data[0] == byte(22), "returned payload mismatch after async PoolItem release"

proc main() =
  testAsyncValueTransfer()
  testAsyncPoolItemTransfer()

main()
echo "OK: async polling bridge tests passed"
