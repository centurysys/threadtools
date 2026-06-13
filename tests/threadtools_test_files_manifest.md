# threadtools test files manifest

Copy each file to the destination path under the `threadtools` repository root.

| Source file | Destination path | Purpose |
|---|---|---|
| test_thread_queue_move_transfer.nim | tests/test_thread_queue_move_transfer.nim | ThreadQueue send/receive/tryReceive/MoveResult/MoveOption pointer-stability runtime test |
| test_pool_item_move_return.nim | tests/test_pool_item_move_return.nim | PoolItem release/destructor/take/double-release/queue-transfer runtime test |
| test_pool_item_thread_pingpong.nim | tests/test_pool_item_thread_pingpong.nim | Cross-thread PoolItem ping-pong runtime test |
| compile_fail_send_move_reuse_after_send.nim | tests/compile_fail/send_move_reuse_after_send.nim | Verifies sendMove consumes the source value |
| compile_fail_new_pool_item_reuse_after_ctor.nim | tests/compile_fail/new_pool_item_reuse_after_ctor.nim | Verifies newPoolItem consumes the source value |
| compile_fail_pool_item_copy.nim | tests/compile_fail/pool_item_copy.nim | Verifies PoolItem cannot be copied |
| compile_fail_send_copy_pool_item.nim | tests/compile_fail/send_copy_pool_item.nim | Verifies sendCopy cannot be used for PoolItem |
| run_threadtools_tests.sh | run_threadtools_tests.sh | Runs runtime tests and compile-fail tests |

Suggested run command:

```sh
./run_threadtools_tests.sh
```

The runner defaults to:

```sh
nim c -r --threads:on --gc:orc --path:src tests/test_*.nim
nim c --threads:on --gc:orc --path:src tests/compile_fail/*.nim
```

Override flags if needed:

```sh
NIM_FLAGS="--threads:on --gc:arc --path:src" ./run_threadtools_tests.sh
NIM_FLAGS="--threads:on --gc:orc -d:release --path:src" ./run_threadtools_tests.sh
```
