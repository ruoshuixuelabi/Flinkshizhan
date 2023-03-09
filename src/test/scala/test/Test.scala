package test

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Test {
  def main(args: Array[String]): Unit = {
    class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
      def process(key: String, context: Context, input: Iterable[(String, Long)], out: Collector[String]) = {
        var count = 0L
        for (in <- input) {
          count = count + 1
        }
        out.collect(s"Window ${context.window} count: $count")

      }
    }
  }
}