import threadtools

type
  Buf = object
    data: seq[byte]

  WorkerCtx = object
    input: ThreadQueue[PoolItem[Buf]]
    output: ThreadQueue[PoolItem[Buf]]
    iterations: int

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

proc workerMain(ctx: WorkerCtx) {.thread.} =
  for i in 0 ..< ctx.iterations:
    var item = ctx.input.receive()
    doAssert item.isActive, "worker received inactive PoolItem"
    doAssert item.item.data.len > 0, "worker received empty buffer"

    item.item.data[0] = byte((int(item.item.data[0]) + 1) mod 251)

    let sent = ctx.output.sendMove(item)
    doAssert sent.isOk, "worker output sendMove failed: " & $sent.error

proc main() =
  const Iterations = 1000

  var returnQ = assertOk(newThreadQueue[Buf](8), "newThreadQueue returnQ failed")
  var toWorker = assertOk(newThreadQueue[PoolItem[Buf]](8), "newThreadQueue toWorker failed")
  var fromWorker = assertOk(newThreadQueue[PoolItem[Buf]](8), "newThreadQueue fromWorker failed")

  var ctx = WorkerCtx(input: toWorker, output: fromWorker, iterations: Iterations)
  var th: Thread[WorkerCtx]
  createThread(th, workerMain, ctx)

  for i in 0 ..< Iterations:
    var buf = makeBuf(16 * 1024, i)
    let before = dataPtr(buf)
    let expected0 = byte((int(buf.data[0]) + 1) mod 251)

    var item = newPoolItem[Buf](returnQ, buf)
    let sent = toWorker.sendMove(item)
    doAssert sent.isOk, "main sendMove to worker failed: " & $sent.error

    var returnedItem = fromWorker.receive()
    doAssert returnedItem.isActive, "main received inactive PoolItem"
    doAssert dataPtr(returnedItem.item) == before, "thread ping-pong changed seq backing pointer"
    doAssert returnedItem.item.data[0] == expected0, "worker mutation was not visible"

    let released = returnedItem.release()
    doAssert released.isOk, "main release after ping-pong failed: " & $released.error

    var returnedBuf = returnQ.receive()
    doAssert dataPtr(returnedBuf) == before, "return queue changed seq backing pointer"
    doAssert returnedBuf.data[0] == expected0, "returned buffer data mismatch"

  joinThread(th)

  echo "OK: pool item thread ping-pong tests passed"

main()
