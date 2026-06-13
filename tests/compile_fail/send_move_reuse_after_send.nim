import threadtools

type
  Buf = object
    data: seq[byte]

proc makeBuf(): Buf =
  result.data = newSeq[byte](1024)

var q = newThreadQueue[Buf](1).expect("newThreadQueue failed")
var buf = makeBuf()

discard q.sendMove(buf)

# This must not compile.  sendMove() consumes buf through ensureMove().
discard buf.data.len
