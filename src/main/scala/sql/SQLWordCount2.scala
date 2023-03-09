package sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.Table

object SQLWordCount2 {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    //2.Source
    import org.apache.flink.streaming.api.scala._
    val input = env.fromElements(
      new WC("Hello", 1),
      new WC("World", 1),
      new WC("Hello", 1)
    )
    //3.注册表
    tEnv.createTemporaryView("WordCount", input, $("word"), $("frequency"))
    //4.执行查询
    val resultTable: Table = tEnv.sqlQuery("SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word")
    //5.输出结果
    //toAppendStream doesn't support consuming update changes which is produced by node GroupAggregate
//    DataStream<WC> resultDS = tEnv.toAppendStream(resultTable, WC.class);
//    val resultDS = tEnv.toRetractStream(resultTable, classOf[WC])
    val resultDS = tEnv.toRetractStream[WC](resultTable)
    resultDS.print
  }
}

case class WC(word: String, frequency: Long)