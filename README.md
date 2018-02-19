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
> 具体的实现方式见： SparkApp

## 使用说明
> 1 . 要将常使用到的jar包上传到HDFS或者其它的存储介质中
> 2 . 如果有自定义的JAR包，可以通过addDependencyJars("file:///localDir/xx.jar") 
> 3 . 如果要加入某个“配置”文件，可以通过addOptions(Files,"/localDir/fileName")

![sss](https://mp.weixin.qq.com/misc/getqrcode?fakeid=3522253881&token=850887939&action=download&style=1&pixsize=336，https://mp.weixin.qq.com/cgi-bin/settingpage?t=setting/index&action=index&token=8508)


