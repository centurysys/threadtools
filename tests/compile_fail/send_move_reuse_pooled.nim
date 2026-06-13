import threadtools

type
  Buf = object
    data: seq[byte]

proc makeBuf(): Buf =
  result.data = newSeq[byte](128)

proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()

var pool = assertOk(newPool[Buf](1), "newPool failed")
discard pool.addMove(makeBuf())
var q = assertOk(newPooledQueue[Buf](1), "newPooledQueue failed")

var item = pool.acquire()
discard q.sendMove(item)

echo item.value.data.len
