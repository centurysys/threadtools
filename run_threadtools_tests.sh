#!/bin/sh
set -eu

NIM=${NIM:-nim}
NIM_FLAGS=${NIM_FLAGS:-"--threads:on --mm:orc --path:src --outdir:build/tests"}

RUNTIME_TESTS="\
tests/test_thread_queue_move_transfer.nim \
tests/test_pool_item_move_return.nim \
tests/test_pool_item_thread_pingpong.nim \
tests/test_async_polling_bridge.nim\
"

COMPILE_FAIL_TESTS="\
tests/compile_fail/send_move_reuse_after_send.nim \
tests/compile_fail/new_pool_item_reuse_after_ctor.nim \
tests/compile_fail/pool_item_copy.nim \
tests/compile_fail/send_copy_pool_item.nim\
"

echo "NIM       : $NIM"
echo "NIM_FLAGS : $NIM_FLAGS"
echo

for t in $RUNTIME_TESTS; do
  echo "== runtime: $t"
  $NIM c -r $NIM_FLAGS "$t"
  echo
 done

for t in $COMPILE_FAIL_TESTS; do
  echo "== compile-fail: $t"
  log="/tmp/threadtools_compile_fail_$(basename "$t").log"

  if $NIM c $NIM_FLAGS "$t" >"$log" 2>&1; then
    cat "$log"
    echo "ERROR: expected compile failure, but compile succeeded: $t" >&2
    exit 1
  fi

  cat "$log"
  echo "compile-fail OK: $t"
  echo
 done

echo "ALL TESTS PASSED"
