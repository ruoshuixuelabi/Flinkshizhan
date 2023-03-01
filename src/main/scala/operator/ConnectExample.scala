package operator

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.functions.co.CoMapFunction

/**
  * connect()算子示例
  */
object ConnectExample {
    def main(args: Array[String]): Unit = {
        //创建流处理的执行环境
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        //创建数据流一
        val dataStream1: DataStream[Int] = senv.fromElements(1, 2, 5, 3)
        //创建数据流二
        val dataStream2: DataStream[String] = senv.fromElements("a", "b", "c", "d")
        //连接两个数据流
        val connectedStream: ConnectedStreams[Int, String] = dataStream1.connect(dataStream2)

        //CoMapFunction三个泛型分别对应第一个流的输入、第二个流的输入，map之后的输出
        class MyCoMapFunction extends CoMapFunction[Int, String, String] {
            //处理第一个流
            override def map1(input1: Int): String = input1.toString
            //处理第二个流
            override def map2(input2: String): String = input2
        }

        //对连接流进行map处理
        val resultDataStream: DataStream[String] = connectedStream.map(new MyCoMapFunction)
        resultDataStream.print()
//      3
//      5
//      a
//      1
//      b
//      2
//      c
//      d
        //执行作业，指定作业名称
        senv.execute("MyJob") //指定作业名称
    }

}
