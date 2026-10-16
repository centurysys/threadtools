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
  doAssert r.isErr, msg & ": expected an error"
  doAssert r.error == expected, msg & ": wrong error: " & $r.error

proc testQueueOwnership() =
  var q = assertOk(newThreadQueue[Buf](1), "newThreadQueue failed")

  var first = makeBuf(64 * 1024, 10)
  let firstPtr = dataPtr(first)
  let sentFirst = q.trySendMove(first)
  doAssert assertOk(sentFirst, "first trySendMove failed")

  var second = makeBuf(64 * 1024, 20)
  let secondPtr = dataPtr(second)
  let secondLen = second.data.len

  # Full queues must return immediately without consuming or modifying data.
  for attempt in 0 ..< 3:
    let full = q.trySendMove(second)
    doAssert not assertOk(full, "full trySendMove failed")
    doAssert second.tag == 20, "full trySendMove changed source tag"
    doAssert second.data.len == secondLen, "full trySendMove changed length"
    doAssert dataPtr(second) == secondPtr, "full trySendMove moved the buffer"
    doAssert second.data[0] == byte(20), "full trySendMove changed content"

  var receivedFirst = q.receive()
  doAssert receivedFirst.tag == 10
  doAssert dataPtr(receivedFirst) == firstPtr

  # The same retained buffer must be sendable as soon as there is capacity.
  let retry = q.trySendMove(second)
  doAssert assertOk(retry, "retry trySendMove failed")
  var receivedSecond = q.receive()
  doAssert receivedSecond.tag == 20
  doAssert receivedSecond.data.len == secondLen
  doAssert dataPtr(receivedSecond) == secondPtr
  doAssert receivedSecond.data[0] == byte(20)

proc testHandleOwnership() =
  var q = assertOk(newThreadQueue[Buf](1), "handle queue creation failed")
  let h = q.handle

  var first = makeBuf(64 * 1024, 30)
  let firstPtr = dataPtr(first)
  doAssert assertOk(h.trySendMove(first), "handle first send failed")

  var second = makeBuf(64 * 1024, 40)
  let secondPtr = dataPtr(second)
  let full = h.trySendMove(second)
  doAssert not assertOk(full, "handle full send failed")
  doAssert second.tag == 40
  doAssert dataPtr(second) == secondPtr

  var receivedFirst = q.receive()
  doAssert dataPtr(receivedFirst) == firstPtr
  doAssert assertOk(h.trySendMove(second), "handle retry failed")
  var receivedSecond = q.receive()
  doAssert receivedSecond.tag == 40
  doAssert dataPtr(receivedSecond) == secondPtr

proc testInvalidAndClosed() =
  var missing: ThreadQueue[Buf]
  var missingHandle: ThreadQueueHandle[Buf]
  var unsent = makeBuf(64 * 1024, 50)
  let ptrBefore = dataPtr(unsent)

  assertError(missing.trySendMove(unsent), ErrorCode.InvalidState,
    "nil ref queue")
  doAssert unsent.tag == 50
  doAssert dataPtr(unsent) == ptrBefore

  assertError(missingHandle.trySendMove(unsent), ErrorCode.InvalidState,
    "nil queue handle")
  doAssert unsent.tag == 50
  doAssert dataPtr(unsent) == ptrBefore

  var q = assertOk(newThreadQueue[Buf](1), "closed queue creation failed")
  let h = q.handle
  q.close()

  assertError(q.trySendMove(unsent), ErrorCode.Closed, "closed ref queue")
  doAssert unsent.tag == 50
  doAssert dataPtr(unsent) == ptrBefore

  assertError(h.trySendMove(unsent), ErrorCode.Closed, "closed queue handle")
  doAssert unsent.tag == 50
  doAssert dataPtr(unsent) == ptrBefore

proc main() =
  testQueueOwnership()
  testHandleOwnership()
  testInvalidAndClosed()
  echo "OK: thread queue non-blocking move send tests passed"

main()
