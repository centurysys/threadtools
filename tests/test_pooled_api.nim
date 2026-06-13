import threadtools

type
  Buf = object
    data: seq[byte]
    tag: int

proc makeBuf(n: int; tag: int): Buf =
  result.data = newSeq[byte](n)
  result.tag = tag

  for i in 0 ..< n:
    result.data[i] = byte((tag + i) mod 251)

proc dataPtr(buf: Buf): uint =
  if buf.data.len == 0:
    return 0'u

  return cast[uint](unsafeAddr buf.data[0])

proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()

proc assertPoolEmpty(pool: Pool[Buf]; msg: string) =
  var item: Pooled[Buf]
  let ret = pool.tryAcquire(item)
  doAssert ret.isOk, msg & ": tryAcquire failed: " & $ret.error
  doAssert not ret.get(), msg

proc main() =
  block:
    var pool = assertOk(newPool[Buf](4), "newPool failed")

    var buf = makeBuf(32 * 1024, 10)
    let before = dataPtr(buf)

    let added = pool.addMove(buf)
    doAssert added.isOk, "pool.addMove failed: " & $added.error

    block:
      var item = pool.acquire()
      doAssert item.isActive, "acquired item should be active"
      doAssert item.value.tag == 10, "acquired item has wrong tag"
      doAssert dataPtr(item.value) == before, "Pool.acquire changed seq backing pointer"

      item.value.tag = 11
      item.value.data[0] = 99'u8

      let rel = item.release()
      doAssert rel.isOk, "Pooled.release failed: " & $rel.error
      doAssert not item.isActive, "released item should be inactive"

    block:
      var item = pool.acquire()
      doAssert item.value.tag == 11, "released item did not return to the pool"
      doAssert item.value.data[0] == 99'u8, "returned payload content mismatch"
      doAssert dataPtr(item.value) == before, "Pooled.release changed seq backing pointer"
      discard item.release()

  block:
    var pool = assertOk(newPool[Buf](4), "newPool auto-return failed")

    var buf = makeBuf(32 * 1024, 20)
    let before = dataPtr(buf)
    discard pool.addMove(buf)

    block:
      var item = pool.acquire()
      doAssert item.isActive, "auto-return item should be active"
      doAssert dataPtr(item.value) == before, "auto-return pointer mismatch before scope exit"
      # No explicit release.  Destructor should return the value to the pool.

    block:
      var item = pool.acquire()
      doAssert dataPtr(item.value) == before, "auto-return changed seq backing pointer"
      doAssert item.value.tag == 20, "auto-return returned wrong item"
      discard item.release()

  block:
    var pool = assertOk(newPool[Buf](4), "newPool queue-transfer failed")
    var toWorker = assertOk(newPooledQueue[Buf](4), "newPooledQueue toWorker failed")
    var fromWorker = assertOk(newPooledQueue[Buf](4), "newPooledQueue fromWorker failed")

    var buf = makeBuf(64 * 1024, 30)
    let before = dataPtr(buf)
    discard pool.addMove(buf)

    var item = pool.acquire()
    doAssert dataPtr(item.value) == before, "pool acquire pointer mismatch before queue send"

    let sent = toWorker.sendMove(item)
    doAssert sent.isOk, "PooledQueue.sendMove failed: " & $sent.error

    var workerItem = toWorker.receive()
    doAssert workerItem.isActive, "worker item should be active"
    doAssert workerItem.value.tag == 30, "worker item has wrong tag"
    doAssert dataPtr(workerItem.value) == before, "PooledQueue.receive changed seq backing pointer"

    workerItem.value.tag = 31
    workerItem.value.data[0] = 77'u8

    let returned = fromWorker.sendMove(workerItem)
    doAssert returned.isOk, "fromWorker.sendMove failed: " & $returned.error

    var mainItem = fromWorker.receive()
    doAssert mainItem.value.tag == 31, "main received wrong modified tag"
    doAssert mainItem.value.data[0] == 77'u8, "main received wrong modified data"
    doAssert dataPtr(mainItem.value) == before, "round-trip queue transfer changed seq backing pointer"

    discard mainItem.release()

    var pooledAgain = pool.acquire()
    doAssert pooledAgain.value.tag == 31, "released queue item did not return to pool"
    doAssert dataPtr(pooledAgain.value) == before, "queue-transfer release changed seq backing pointer"
    discard pooledAgain.release()

  block:
    var pool = assertOk(newPool[Buf](1), "newPool tryAcquire failed")
    assertPoolEmpty(pool, "new pool should be empty before addMove")

    var buf = makeBuf(1024, 40)
    discard pool.addMove(buf)

    var item: Pooled[Buf]
    let got = pool.tryAcquire(item)
    doAssert got.isOk, "tryAcquire failed: " & $got.error
    doAssert got.get(), "tryAcquire should have received an item"
    doAssert item.value.tag == 40, "tryAcquire returned wrong item"

    var second: Pooled[Buf]
    let empty = pool.tryAcquire(second)
    doAssert empty.isOk, "empty tryAcquire failed: " & $empty.error
    doAssert not empty.get(), "pool should be empty while item is active"

    discard item.release()

  echo "OK: pooled API tests passed"

main()
