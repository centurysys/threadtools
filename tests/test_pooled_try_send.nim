import threadtools

type
  Buf = object
    data: seq[byte]
    tag: int

proc makeBuf(n: int; tag: int): Buf =
  result.data = newSeq[byte](n)
  result.tag = tag
  for i in 0 ..< n:
    result.data[i] = byte((i + tag) mod 251)

proc dataPtr(buf: Buf): uint =
  if buf.data.len == 0:
    return 0'u
  return cast[uint](unsafeAddr buf.data[0])

proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()

proc assertError[T](r: Result[T, ErrorCode]; expected: ErrorCode;
    msg: string) =
  doAssert r.isErr, msg & ": expected error"
  doAssert r.error == expected, msg & ": wrong error: " & $r.error

proc assertPoolEmpty(pool: Pool[Buf]; msg: string) =
  var candidate: Pooled[Buf]
  let received = pool.tryAcquire(candidate)
  doAssert received.isOk, msg & ": tryAcquire failed"
  doAssert not received.get(), msg & ": unexpected free buffer"

proc testTryAddMove() =
  var pool = assertOk(newPool[Buf](1), "newPool for tryAddMove")

  var first = makeBuf(64 * 1024, 10)
  let firstPtr = dataPtr(first)
  doAssert assertOk(pool.tryAddMove(first), "initial tryAddMove")

  var second = makeBuf(64 * 1024, 20)
  let secondPtr = dataPtr(second)
  for attempt in 0 ..< 3:
    let full = pool.tryAddMove(second)
    doAssert not assertOk(full, "full pool tryAddMove")
    doAssert second.tag == 20, "full pool changed caller tag"
    doAssert second.data.len == 64 * 1024, "full pool changed length"
    doAssert second.data[0] == 20'u8, "full pool changed payload"
    doAssert dataPtr(second) == secondPtr, "full pool changed backing pointer"

  var firstItem = pool.acquire()
  doAssert firstItem.value.tag == 10
  doAssert dataPtr(firstItem.value) == firstPtr

  # Take the first item out of the pool rather than releasing into an occupied
  # free queue later; this also avoids an auto-return in the test destructor.
  var taken = firstItem.take()
  doAssert taken.isOk, "first pooled item take failed"
  var firstOwned = taken.take()
  doAssert firstOwned.tag == 10
  doAssert dataPtr(firstOwned) == firstPtr

  doAssert assertOk(pool.tryAddMove(second), "retry pool tryAddMove")
  var secondItem = pool.acquire()
  doAssert secondItem.isActive
  doAssert secondItem.value.tag == 20
  doAssert secondItem.value.data[0] == 20'u8
  doAssert dataPtr(secondItem.value) == secondPtr
  doAssert assertOk(secondItem.release(), "return second item to pool")

  var recovered = pool.acquire()
  doAssert dataPtr(recovered.value) == secondPtr
  doAssert assertOk(recovered.release(), "return recovered item")

proc testPooledQueueTrySendMove() =
  var pool = assertOk(newPool[Buf](2), "newPool for pooled queue")
  var destination = assertOk(newPooledQueue[Buf](1), "newPooledQueue")

  var first = makeBuf(64 * 1024, 30)
  var second = makeBuf(64 * 1024, 40)
  let firstPtr = dataPtr(first)
  let secondPtr = dataPtr(second)
  doAssert assertOk(pool.tryAddMove(first), "add first buffer")
  doAssert assertOk(pool.tryAddMove(second), "add second buffer")

  var firstItem = pool.acquire()
  var secondItem = pool.acquire()
  doAssert dataPtr(firstItem.value) == firstPtr
  doAssert dataPtr(secondItem.value) == secondPtr
  assertPoolEmpty(pool, "both items acquired")

  doAssert assertOk(destination.trySendMove(firstItem), "send first pooled item")

  # Repeated failed sends must not run PoolItem's auto-return destructor, nor
  # change the active ownership token held by the caller.
  for attempt in 0 ..< 3:
    let full = destination.trySendMove(secondItem)
    doAssert not assertOk(full, "full pooled queue trySendMove")
    doAssert secondItem.isActive, "full send deactivated pooled item"
    doAssert secondItem.value.tag == 40, "full send changed pooled tag"
    doAssert secondItem.value.data.len == 64 * 1024
    doAssert secondItem.value.data[0] == 40'u8
    doAssert dataPtr(secondItem.value) == secondPtr, "full send copied buffer"
    assertPoolEmpty(pool, "failed send must not auto-return an active item")

  var receivedFirst = destination.receive()
  doAssert receivedFirst.isActive
  doAssert receivedFirst.value.tag == 30
  doAssert dataPtr(receivedFirst.value) == firstPtr

  doAssert assertOk(destination.trySendMove(secondItem), "retry pooled send")
  var receivedSecond = destination.receive()
  doAssert receivedSecond.isActive
  doAssert receivedSecond.value.tag == 40
  doAssert dataPtr(receivedSecond.value) == secondPtr

  # Exactly two buffers must return to their original pool, not duplicates.
  doAssert assertOk(receivedFirst.release(), "release first received item")
  doAssert assertOk(receivedSecond.release(), "release second received item")

  var recoveredFirst = pool.acquire()
  var recoveredSecond = pool.acquire()
  doAssert dataPtr(recoveredFirst.value) == firstPtr
  doAssert dataPtr(recoveredSecond.value) == secondPtr
  assertPoolEmpty(pool, "both items recovered, no extra returns")
  doAssert assertOk(recoveredFirst.release(), "release recovered first")
  doAssert assertOk(recoveredSecond.release(), "release recovered second")

proc testInvalidAndClosed() =
  var missingPool: Pool[Buf]
  var buffer = makeBuf(64 * 1024, 50)
  let originalPtr = dataPtr(buffer)
  assertError(missingPool.tryAddMove(buffer), ErrorCode.InvalidState,
    "uninitialized pool")
  doAssert dataPtr(buffer) == originalPtr

  var closedPool = assertOk(newPool[Buf](1), "closed pool creation")
  closedPool.close()
  assertError(closedPool.tryAddMove(buffer), ErrorCode.Closed, "closed pool")
  doAssert buffer.tag == 50
  doAssert dataPtr(buffer) == originalPtr

  var pool = assertOk(newPool[Buf](1), "source pool")
  doAssert assertOk(pool.tryAddMove(buffer), "source pool add")
  var item = pool.acquire()
  doAssert item.isActive
  doAssert dataPtr(item.value) == originalPtr

  var missingQueue: PooledQueue[Buf]
  assertError(missingQueue.trySendMove(item), ErrorCode.InvalidState,
    "uninitialized pooled queue")
  doAssert item.isActive
  doAssert dataPtr(item.value) == originalPtr

  var closedQueue = assertOk(newPooledQueue[Buf](1), "closed pooled queue")
  closedQueue.close()
  assertError(closedQueue.trySendMove(item), ErrorCode.Closed,
    "closed pooled queue")
  doAssert item.isActive
  doAssert dataPtr(item.value) == originalPtr

  var openQueue = assertOk(newPooledQueue[Buf](1), "active pooled queue")
  var inactive: Pooled[Buf]
  assertError(openQueue.trySendMove(inactive), ErrorCode.InvalidState,
    "inactive pooled item")
  assertPoolEmpty(pool, "invalid sends must retain the original item")

  doAssert assertOk(item.release(), "release after failed queue sends")
  var recovered = pool.acquire()
  doAssert recovered.isActive
  doAssert dataPtr(recovered.value) == originalPtr
  doAssert assertOk(recovered.release(), "return recovered item")

proc main() =
  testTryAddMove()
  testPooledQueueTrySendMove()
  testInvalidAndClosed()
  echo "OK: pool and pooled queue non-blocking send tests passed"

main()
