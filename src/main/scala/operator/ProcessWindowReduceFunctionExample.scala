package operator

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * 每隔5秒计算每个用户的最小成绩
 */
object ProcessWindowReduceFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //数据源
    var stream = env.socketTextStream("localhost", 9999)
    //用户ID,成绩
    //1001,99
    //1002,87
    //1001,76
    //1001,88
    //1002,69
    stream.map(line => {
      var arr = line.split(",")
      (arr(0), arr(1).toInt)
    })
      .keyBy(_._1) //根据第一个字段（此处指用户ID）分组
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce(new MyReduceFunction2, new MyProcessWindowFunction2) //指定预聚合函数和窗口函数
      .print()
    env.execute("ProcessWindowFunctionExample")
  }
  //使用增量聚合函数进行预聚合
  class MyReduceFunction2() extends ReduceFunction[(String, Int)] {
    override def reduce(r1: (String, Int), r2: (String, Int)): (String, Int) = {
      if (r1._2 > r2._2)
        (r1._1, r2._2)
      else
        (r1._1, r1._2)
    }
  }

  //使用全量聚合函数获取窗口开始时间
  //ReduceFunction计算完毕后，会将每组数据（以Key分组）的计算结果输入到ProcessWindowFunction的process()方法中
  class MyProcessWindowFunction2() extends ProcessWindowFunction[(String, Int), (String, String), String, TimeWindow] {
    //窗口结束的时候调用（每次传入一个分组的数据）
    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, String)]): Unit = {
      //从elements变量中获取最小值，本例该变量中只有一条数据，即最小值
      val min = elements.iterator.next().toString()
      //输出窗口开始时间与最小值
      out.collect(context.window.getStart.toString, min)
    }
  }
}