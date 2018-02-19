# sparkdeploy
## spark 部署模式之自定义＋一键提交
如：
    object MonitorEventStreamingRunner extends App {
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
  
  只需extends app 邮件执行既可以将job提交到yarn-cluster 上面执行
