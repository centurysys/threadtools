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

proc main() =
  var q = assertOk(newThreadQueue[Buf](4), "newThreadQueue failed")

  block:
    var buf = makeBuf(64 * 1024, 1)
    let before = dataPtr(buf)

    let sent = q.sendMove(buf)
    doAssert sent.isOk, "sendMove failed: " & $sent.error

    var received = q.receive()
    doAssert dataPtr(received) == before, "receive changed seq backing pointer"
    doAssert received.data[0] == byte(1)

  block:
    var buf = makeBuf(64 * 1024, 2)
    let before = dataPtr(buf)

    let sent = q.sendMove(buf)
    doAssert sent.isOk, "sendMove before receiveChecked failed: " & $sent.error

    var receivedResult = q.receiveChecked()
    doAssert receivedResult.isOk, "receiveChecked returned Err: " & $receivedResult.error

    var received = receivedResult.take()
    doAssert dataPtr(received) == before, "receiveChecked/MoveResult.take changed seq backing pointer"
    doAssert received.data[0] == byte(2)

  block:
    var buf = makeBuf(64 * 1024, 3)
    let before = dataPtr(buf)

    let sent = q.sendMove(buf)
    doAssert sent.isOk, "sendMove before tryReceive failed: " & $sent.error

    var received: Buf
    var tryRet = q.tryReceive(received)
    doAssert tryRet.isOk, "tryReceive returned Err: " & $tryRet.error
    doAssert tryRet.get(), "tryReceive reported empty queue"
    doAssert dataPtr(received) == before, "tryReceive(var out) changed seq backing pointer"
    doAssert received.data[0] == byte(3)

  block:
    var buf = makeBuf(64 * 1024, 4)
    let before = dataPtr(buf)

    let sent = q.sendMove(buf)
    doAssert sent.isOk, "sendMove before tryReceiveMove failed: " & $sent.error

    var moveResult = q.tryReceiveMove()
    doAssert moveResult.isOk, "tryReceiveMove returned Err: " & $moveResult.error

    var opt = moveResult.take()
    doAssert opt.isSome, "tryReceiveMove returned None for queued value"

    var received = opt.take()
    doAssert dataPtr(received) == before, "tryReceiveMove/MoveOption.take changed seq backing pointer"
    doAssert received.data[0] == byte(4)

  block:
    var moveResult = q.tryReceiveMove()
    doAssert moveResult.isOk, "tryReceiveMove empty returned Err: " & $moveResult.error

    var opt = moveResult.take()
    doAssert opt.isNone, "tryReceiveMove empty returned Some"

  block:
    var checked = q.tryReceiveChecked()
    doAssert checked.isErr, "tryReceiveChecked empty should return Err(Empty)"
    doAssert checked.error == ErrorCode.Empty, "tryReceiveChecked empty returned wrong error: " & $checked.error

  echo "OK: thread queue move transfer tests passed"

main()
