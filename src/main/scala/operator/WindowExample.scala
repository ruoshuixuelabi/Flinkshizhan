package operator

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * 单词计数窗口计算,每隔5秒统计一次
  */
object WindowExample {
    def main(args: Array[String]): Unit = {
        //第一步：创建流处理的执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //第二步：读取流数据
        val text = env.socketTextStream("localhost", 9999)
        //第三步：转换数据
        val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
          .map { (_, 1) }
          .keyBy(_._1)
          .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
          .sum(1)
        //第四步：输出结果到控制台
        counts.print()
        //第五步：触发任务执行
        env.execute("Window Stream WordCount")
    }

}

