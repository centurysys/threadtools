import std/asyncdispatch
import threadtools

# Close/cancel tests intentionally avoid async proc for move-only payloads.
# waitFor() is used only with Future[AsyncOwned[T]], whose Future payload is a
# ref box.  The actual owned payload is taken explicitly with AsyncOwned.take().

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

proc expectAsyncBridgeError[T](fut: Future[AsyncOwned[T]]; msg: string) =
  var raised = false

  try:
    discard waitFor fut
  except ThreadQueueAsyncError:
    raised = true
  except CatchableError as e:
    doAssert false, msg & ": unexpected exception type: " & $e.name & ": " & e.msg

  doAssert raised, msg & ": expected ThreadQueueAsyncError"

proc testCloseFailsPendingReceives() =
  var q = assertOk(newThreadQueue[Buf](8), "newThreadQueue[Buf] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Buf](q), "newAsyncThreadQueueBridge[Buf] failed")

  let pending1 = bridge.recvAsync()
  let pending2 = bridge.recvAsync()

  doAssert not pending1.finished, "pending1 should not finish before close"
  doAssert not pending2.finished, "pending2 should not finish before close"

  bridge.close()

  expectAsyncBridgeError(pending1, "pending1 after close")
  expectAsyncBridgeError(pending2, "pending2 after close")

proc testRecvAfterCloseFailsImmediately() =
  var q = assertOk(newThreadQueue[Buf](8), "newThreadQueue[Buf] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Buf](q), "newAsyncThreadQueueBridge[Buf] failed")

  bridge.close()

  let pending = bridge.recvAsync()
  doAssert pending.finished, "recvAsync after close should return an already failed future"
  expectAsyncBridgeError(pending, "recvAsync after close")

proc testNotifierAfterCloseFails() =
  var q = assertOk(newThreadQueue[Buf](8), "newThreadQueue[Buf] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Buf](q), "newAsyncThreadQueueBridge[Buf] failed")
  let tx = bridge.notifier()

  doAssert tx.isValid, "notifier should be valid before bridge.close"

  bridge.close()

  doAssert not tx.isValid, "notifier should become invalid after bridge.close"

  let ret = tx.notify()
  doAssert ret.isErr, "notify after close should fail"
  doAssert ret.error == ErrorCode.Closed, "notify after close should return ErrorCode.Closed"

proc testCancelPendingDoesNotCloseBridge() =
  var q = assertOk(newThreadQueue[Buf](8), "newThreadQueue[Buf] failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Buf](q), "newAsyncThreadQueueBridge[Buf] failed")
  let tx = bridge.notifier()

  let pending = bridge.recvAsync()
  doAssert not pending.finished, "pending receive should not finish before cancelPending"

  let cancelled = bridge.cancelPending("test cancel")
  doAssert cancelled == 1, "cancelPending should report one cancelled future"
  expectAsyncBridgeError(pending, "pending receive after cancelPending")

  # The bridge remains usable after cancelling current waiters.
  var buf = makeBuf(64 * 1024, 61)
  let before = dataPtr(buf)

  let pending2 = bridge.recvAsync()
  let sent = tx.sendMove(buf)
  doAssert sent.isOk, "sendMove after cancelPending failed: " & $sent.error

  let box = waitFor pending2
  var received = takeOk(box, "AsyncOwned[Buf].take after cancelPending failed")
  doAssert dataPtr(received) == before, "cancelPending path changed seq backing pointer"
  doAssert received.data[0] == byte(61), "cancelPending path returned wrong payload"

  bridge.close()

proc main() =
  testCloseFailsPendingReceives()
  testRecvAfterCloseFailsImmediately()
  testNotifierAfterCloseFails()
  testCancelPendingDoesNotCloseBridge()

main()
echo "OK: async bridge close/cancel tests passed"
