package demo

import org.apache.flink.streaming.api.scala._

/**
 * 单词计数流处理
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //第一步：创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //第二步：读取流数据
    val data: DataStream[String] = env.socketTextStream("172.18.30.87", 9999)
    //第三步：转换数据
    val result: DataStream[(String, Int)] = data.flatMap(_.split(" "))
      .filter(_.nonEmpty) //过滤空字段
      .map((_, 1)) //转换成（单词,1）类型
      .keyBy(_._1) //按照key对数据重分区
      .sum(1) //执行求和运算
    //第四步：输出结果到控制台
    result.print()
    println(env.getExecutionPlan)//打印计划描述
    //第五步：触发任务执行
    env.execute("StreamWordCount") //指定作业名称
  }
}