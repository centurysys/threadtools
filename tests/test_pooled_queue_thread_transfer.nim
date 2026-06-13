import std/typedthreads

import threadtools

type
  Buf = object
    data: seq[byte]
    tag: int

  WorkerArgs = object
    input: PooledQueue[Buf]
    output: PooledQueue[Buf]

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

proc worker(args: WorkerArgs) {.thread.} =
  var item = args.input.receive()
  doAssert item.isActive, "worker received inactive item"

  item.value.tag = item.value.tag + 1
  item.value.data[0] = byte(item.value.tag)

  let ret = args.output.sendMove(item)
  doAssert ret.isOk, "worker output sendMove failed: " & $ret.error

proc main() =
  var pool = assertOk(newPool[Buf](2), "newPool failed")
  var toWorker = assertOk(newPooledQueue[Buf](2), "newPooledQueue toWorker failed")
  var fromWorker = assertOk(newPooledQueue[Buf](2), "newPooledQueue fromWorker failed")

  var buf = makeBuf(64 * 1024, 50)
  let before = dataPtr(buf)
  discard pool.addMove(buf)

  var th: Thread[WorkerArgs]
  createThread(th, worker, WorkerArgs(input: toWorker, output: fromWorker))

  var item = pool.acquire()
  doAssert dataPtr(item.value) == before, "pool acquire changed seq backing pointer"

  let sent = toWorker.sendMove(item)
  doAssert sent.isOk, "toWorker.sendMove failed: " & $sent.error

  var returned = fromWorker.receive()
  doAssert returned.value.tag == 51, "worker did not modify tag"
  doAssert returned.value.data[0] == 51'u8, "worker did not modify data"
  doAssert dataPtr(returned.value) == before, "thread transfer changed seq backing pointer"

  discard returned.release()

  joinThread(th)

  var pooledAgain = pool.acquire()
  doAssert pooledAgain.value.tag == 51, "released item did not return to pool"
  doAssert dataPtr(pooledAgain.value) == before, "release after thread transfer changed pointer"
  discard pooledAgain.release()

  echo "OK: pooled queue thread transfer test passed"

main()
