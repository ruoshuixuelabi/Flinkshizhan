package partition

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._

/**
  * Key分区策略
  */
object KeyGroupStreamExample {
    def main(args: Array[String]): Unit = {
        val env=StreamExecutionEnvironment.getExecutionEnvironment
        //设置当前作业所有算子的并行度为3
        env.setParallelism(3)
        //创建DataStream
        val dataStream:DataStream[(Int,Int)] = env.fromElements(
            (10023,1),
            (100,2),
            (200,1),
            (200,2),
            (300,1),
            (300,2)
        )
        //1.分区策略前的操作---------------------
        //输出dataStream每个元素及所属的子任务编号，编号范围[0,并行度-1]
        val dataStreamOne=dataStream.map(new RichMapFunction[(Int,Int),(Int,Int)] {
            override def map(value: (Int,Int)): (Int,Int) = {
                println(
                    "元素值："+value+"。分区策略前,子任务编号："+getRuntimeContext().getIndexOfThisSubtask()
                )
                value
            }
        })
        //2.分区策略---------------------
        //设置DataStream向下游发送数据时使用广播分区策略
//        val dataStreamTwo=dataStreamOne.broadcast
//        val dataStreamTwo=dataStreamOne.forward
        val dataStreamTwo=dataStreamOne.keyBy(_._1)
        //3.分区策略后的操作---------------------
        dataStreamTwo.map(new RichMapFunction[(Int,Int),(Int,Int)] {
            override def map(value: (Int,Int)): (Int,Int) = {
                println(
                    "元素值："+value+"。分区策略后,子任务编号："+getRuntimeContext().getIndexOfThisSubtask()
                )
                value
            }
        }).print()

        env.execute("KeyGroupStreamExample");
    }
}
