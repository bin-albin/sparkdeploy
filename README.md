# sparkdeploy
## spark 部署模式之自定义＋一键提交
如:
<pre><code>object MonitorEventStreamingRunner extends App {
        val workerNum = "4"
        val classname = "cn.ce.bigdata.sparkstreaming.MonitorEventStreaming"
        SparkApp(classname)
          .addOptions(Master, "yarn-cluster")
          .addOptions(Name, "MonitorEventStreaming")
          .addOptions(ExecutorMemory, "6G")
          .addOptions(NumExecutors, workerNum)
          .addDependencyGroup(HBase)
          .addDependencyGroup(StreamingKafka)
          .run()
      }
</code></pre>
只需extends app 执行run既可以将job提交到yarn-cluster 上面执行
> 具体的实现方式见：
> SparkApp.scala 

