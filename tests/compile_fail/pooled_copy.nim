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

var item1 = pool.acquire()
var item2 = item1

discard item2.release()
