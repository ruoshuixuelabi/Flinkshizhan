package operator

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * 自定义flatMap处理函数
  */
class CountAverage extends RichFlatMapFunction[(Long,Long),(Long,Long)]{

    //1、声明状态对象ValueState，用来存放状态数据
    private var sum: ValueState[(Long, Long)] = _
    //2、状态初始化，初始化调用（默认生命周期方法）
    override def open(parameters: Configuration): Unit = {
        sum = getRuntimeContext.getState(
        //创建状态描述器
        new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
        )
    }
    //3、使用状态（重写flatMap函数）
    override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {
        //访问状态值
        val tmpCurrentSum = sum.value
        //如果状态值为空，赋默认值(0L, 0L)
        val currentSum = if (tmpCurrentSum != null) {
            tmpCurrentSum
        } else {
            (0L, 0L)
        }
        //更新计数，（数量，总和）
        val newSum = (currentSum._1 + 1, currentSum._2 + input._2)
        //更新状态
        sum.update(newSum)
        //如果计数达到2，则发射平均值并清除状态
        if (newSum._1 >= 2) {
            out.collect((input._1, newSum._2 / newSum._1))
            sum.clear()
        }
    }

}

/**
  * 相同Key一旦出现次数达到2，则将其平均值发送到下游，并清除状态重新开始
  */
object ValueStateExample {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.fromCollection(List(
            (1L, 3L),
            (1L, 5L),
            (1L, 7L),
            (1L, 4L),
            (1L, 2L)
        )).keyBy(_._1)
          .flatMap(new CountAverage())//自定义函数
          .print()
        //输出结果为(1,4)和(1,5)

        env.execute("ExampleKeyedState")
    }
}
