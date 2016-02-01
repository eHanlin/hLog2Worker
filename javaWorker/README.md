com.eHanlin.hLog2.worker.JavaWorkerManager

是用來管理 Runnable 使用的，
使用 JavaWorkerManager 的 Worker，
會用 JVM 的 ShutdownHook 來處理關閉的事件。

例子：

```shell

java -jar worker.jar > /tmp/worker.log &
console : [1] 12345
log : Worker start
kill 12345
log : JVM start shutdown
log : Worker start shutdown
log : Worker stop
log : Worker finish shutdown
log : JVM finish shutdown

```