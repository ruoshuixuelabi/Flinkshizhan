//package operator
//
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//
///**
//  * maxBy算子
//  */
//object MaxByExample {
//    def main(args: Array[String]): Unit = {
//        //建流处理的执行环境
//        val senv = StreamExecutionEnvironment.getExecutionEnvironment
//        /* val data:DataStream[String]=senv
//           .readTextFile("hdfs://centos01:9000/input/words.txt")*/
//
//
//        val tupleStream = senv.fromElements(
//            (0, 0, 0), (0, 1, 1), (0, 2, 2),
//            (1, 0, 6), (1, 1, 7), (1, 2, 8)
//        )
//
//        val sumStream = tupleStream
//            .keyBy(_._1)
//            .maxBy(1)
//
//        sumStream.print()
//        // 按第一个字段分组，对第二个字段求和，打印出来的结果如下：
//        //  (0,0,0)
//        //  (0,1,0)
//        //  (0,3,0)
//        //  (1,0,6)
//        //  (1,1,6)
//        //  (1,3,6)
//
//        senv.execute("StreamWordCount") //指定作业名称
//    }
//}
