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

proc assertQueueEmpty(q: ThreadQueue[Buf]; msg: string) =
  var outValue: Buf
  var ret = q.tryReceive(outValue)
  doAssert ret.isOk, msg & ": tryReceive failed: " & $ret.error
  doAssert not ret.get(), msg

proc main() =
  var returnQ = assertOk(newThreadQueue[Buf](8), "newThreadQueue returnQ failed")

  block:
    var buf = makeBuf(32 * 1024, 10)
    let before = dataPtr(buf)

    block:
      var item = newPoolItem[Buf](returnQ, buf)
      doAssert item.isActive, "new PoolItem should be active"
      doAssert dataPtr(item.item) == before, "newPoolItem changed seq backing pointer"

      var rel = item.release()
      doAssert rel.isOk, "PoolItem.release failed: " & $rel.error
      doAssert rel.get(), "PoolItem.release returned false"
      doAssert not item.isActive, "released PoolItem should be inactive"

    var returned = returnQ.receive()
    doAssert dataPtr(returned) == before, "PoolItem.release changed seq backing pointer"
    doAssert returned.data[0] == byte(10)

  block:
    var buf = makeBuf(32 * 1024, 20)
    let before = dataPtr(buf)

    block:
      var item = newPoolItem[Buf](returnQ, buf)
      doAssert item.isActive, "auto-return PoolItem should be active before scope exit"
      doAssert dataPtr(item.item) == before, "auto-return PoolItem pointer mismatch before scope exit"
      # No explicit release.  The destructor should return the value.

    var returned = returnQ.receive()
    doAssert dataPtr(returned) == before, "PoolItem destructor auto-return changed seq backing pointer"
    doAssert returned.data[0] == byte(20)

  block:
    var buf = makeBuf(32 * 1024, 30)
    let before = dataPtr(buf)

    block:
      var item = newPoolItem[Buf](returnQ, buf)
      var takenResult = item.take()
      doAssert takenResult.isOk, "PoolItem.take failed: " & $takenResult.error

      var taken = takenResult.take()
      doAssert dataPtr(taken) == before, "PoolItem.take changed seq backing pointer"
      doAssert taken.data[0] == byte(30)
      doAssert not item.isActive, "taken PoolItem should be inactive"
      # Scope exit must not auto-return after take().

    assertQueueEmpty(returnQ, "PoolItem.take should disable destructor auto-return")

  block:
    var buf = makeBuf(32 * 1024, 40)
    let before = dataPtr(buf)

    block:
      var item = newPoolItem[Buf](returnQ, buf)
      var first = item.release()
      doAssert first.isOk, "first PoolItem.release failed: " & $first.error

      var second = item.release()
      doAssert second.isErr, "second PoolItem.release should fail"
      doAssert second.error == ErrorCode.DoubleRelease, "wrong double-release error: " & $second.error

    var returned = returnQ.receive()
    doAssert dataPtr(returned) == before, "double-release test returned wrong buffer"
    assertQueueEmpty(returnQ, "double-release should not return a second buffer")

  block:
    var itemQ = assertOk(newThreadQueue[PoolItem[Buf]](8), "newThreadQueue itemQ failed")
    var buf = makeBuf(32 * 1024, 50)
    let before = dataPtr(buf)

    var item = newPoolItem[Buf](returnQ, buf)
    let sent = itemQ.sendMove(item)
    doAssert sent.isOk, "sendMove(PoolItem) failed: " & $sent.error

    var movedItem = itemQ.receive()
    doAssert movedItem.isActive, "received PoolItem should still be active"
    doAssert dataPtr(movedItem.item) == before, "PoolItem queue transfer changed seq backing pointer"

    var rel = movedItem.release()
    doAssert rel.isOk, "received PoolItem.release failed: " & $rel.error

    var returned = returnQ.receive()
    doAssert dataPtr(returned) == before, "PoolItem queue transfer release changed seq backing pointer"
    doAssert returned.data[0] == byte(50)

  echo "OK: pool item move/return tests passed"

main()
