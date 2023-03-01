package operator

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 每隔5分钟统计每个用户产生的日志数量（全量聚合）
  */
object ProcessWindowFunctionExample {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //流数据源
        var stream = env.socketTextStream("localhost", 9999)
        //用户ID,日志内容
        //1001,login
        //1002,add
        //1001,update
        //1001,delete
        //将数据流元素类型转为元组
        stream.map(line => {
            var arr = line.split(",")
            (arr(0), arr(1))
        })
          .keyBy(_._1) //根据第一个字段（此处指用户ID）分组
          //定义滚动窗口
          .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
          .process(new MyProcessWindowFunction) //指定窗口处理函数
          .print()//结果打印到控制台

        //执行计算任务
        env.execute("ProcessWindowFunctionExample")
    }
}

/**
  * 定义全量聚合窗口处理类
  */
class MyProcessWindowFunction extends ProcessWindowFunction[(String, String), (String, Int), String, TimeWindow] {
    /**
      * 窗口结束的时候调用（每次传入一个分组的数据）
      * @param key 用户ID
      * @param context 窗口上下文
      * @param elements 某个用户的日志数据
      * @param out 收集计算结果并发射出去
      */
    override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[(String, Int)]): Unit = {
        //全量聚合，整个窗口的数据都保存到了Iterable类型的elements变量中，elements中有很多行数据, elements的size就是日志的总行数
        out.collect(key, elements.size)
    }

}
