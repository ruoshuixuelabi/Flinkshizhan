package partition

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._

/**
  * Shuffle分区策略
  */
object ShuffleExample {
    def main(args: Array[String]): Unit = {
        val env=StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(3)
        val dataStream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6,7, 8, 9, 10, 11, 12)

        val dataStreamOne=dataStream.map(new RichMapFunction[Int,Int] {
            override def map(value: Int): Int = {
                println(
                    "元素值："+value+"。分区策略前,子任务编号："+getRuntimeContext().getIndexOfThisSubtask()
                )
                value
            }
        }).setParallelism(3)
        //设置DataStream向下游发送数据时使用shuffle分区策略
        val dataStreamTwo=dataStreamOne.shuffle
        dataStreamTwo.map(new RichMapFunction[Int,Int] {
            override def map(value: Int): Int = {
                println(
                    "元素值："+value+"。分区策略后,子任务编号："+getRuntimeContext().getIndexOfThisSubtask()
                )
                value
            }
        }).setParallelism(3)
          .print()

        env.execute("ShuffleExample");
    }

}
