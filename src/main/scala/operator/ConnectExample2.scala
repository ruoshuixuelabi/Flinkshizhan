package operator

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  * connect()算子示例
  */
object ConnectExample2 {
    def main(args: Array[String]): Unit = {
        StreamExecutionEnvironment.setDefaultLocalParallelism(2)
        //创建流处理的执行环境
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        //创建数据流一
        val dataStream1: DataStream[Int] = senv.fromElements(1, 2, 5, 3)
        //创建数据流二
        val dataStream2: DataStream[String] = senv.fromElements("a", "b", "c", "d")
        //连接两个数据流
        val connectedStream: ConnectedStreams[Int, String] = dataStream1.connect(dataStream2.broadcast)

        dataStream2.print()
//        val jobClient=senv.executeAsync()
//        val jobExecutionResult = jobClient.getJobExecutionResult().get()


        senv.execute("MyJob") //指定作业名称


    }

}
