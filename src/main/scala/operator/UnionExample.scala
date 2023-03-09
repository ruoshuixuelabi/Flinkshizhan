package operator

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, _}

/**
  * union()算子示例
  */
object UnionExample {
    def main(args: Array[String]): Unit = {
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        //创建数据流一
        val dataStream1 = senv.fromElements(
            (0, 0, 0), (1, 1, 1), (2, 2, 2)
        )
        //创建数据流二
        val dataStream2 = senv.fromElements(
            (3, 3, 3), (4, 4, 4), (5, 5, 5)
        )
        //合并两个数据流
        val unionDataStream=dataStream1.union(dataStream2)
        dataStream1.broadcast()
        unionDataStream.print()
//        (0,0,0)
//        (5,5,5)
//        (4,4,4)
//        (2,2,2)
//        (1,1,1)
//        (3,3,3)


    }

}
