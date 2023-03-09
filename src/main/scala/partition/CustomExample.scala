package partition

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * 自定义分区策略
 */
object CustomExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    //构建模拟数据
    val arr = Array(
      "chinese,98",
      "math,88",
      "english,96"
    )
    val dataStream: DataStream[String] = env.fromCollection(arr)
    //1.分区策略前的操作---------------------
    //输出dataStream每个元素及所属的子任务编号，编号范围[0,并行度-1]
    val dataStreamOne = dataStream.map(new RichMapFunction[String, (String, Int)] {
      override def map(value: String): (String, Int) = {
        println(
          "元素值：" + value + "。分区策略前,子任务编号："
            + getRuntimeContext().getIndexOfThisSubtask()
        )
        (value.split(",")(0), value.split(",")(1).toInt)
      }
    }).setParallelism(2)
    //2.设置分区策略----------------------------
    //设置DataStream向下游发送数据时使用自定义分区策略，同时将DataStream元素的第一个字段作为分区Key
    val dataStreamTwo = dataStreamOne.partitionCustom(new MyPartitioner, value => value._1)
    //3.分区策略后的操作---------------------
    //输出dataStream每个元素及所属的子任务编号，编号范围[0,并行度-1]
    dataStreamTwo.map(new RichMapFunction[(String, Int), (String, Int)] {
      override def map(value: (String, Int)): (String, Int) = {
        println(
          "元素值：" + value + "。分区策略后,子任务编号："
            + getRuntimeContext().getIndexOfThisSubtask()
        )
        value
      }
    }).setParallelism(3)
      .print()
    env.execute("ShuffleExample");
  }
}