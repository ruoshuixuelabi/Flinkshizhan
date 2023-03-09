package sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{$, EnvironmentSettings}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._

/**
 * Flink SQL统计订单流数据
 * 知识点：DataStream转为Table、视图，Table转为DataStream，SQL查询
 */
object Demo01_SQL_Simple {
  def main(args: Array[String]): Unit = {
    //创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建EnvironmentSettings实例并设置参数
    val settings = EnvironmentSettings
      .newInstance() //创建一个用于创建EnvironmentSettings实例的构建器
//      .useBlinkPlanner() //将Blink计划器设置为所需的模块（默认）
      .inStreamingMode() //设置组件以流模式工作。默认启用
      .build() //创建一个不可变的EnvironmentSettings实例
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //构建订单数据流A
    val orderStreamA: DataStream[Order] = env.fromCollection(
      List(Order(1L, "尺子", 3),
        Order(1L, "铅笔", 4),
        Order(3L, "橡皮", 2)
      )
    )
    //构建订单数据流B
    val orderStreamB: DataStream[Order] = env.fromCollection(
      List(Order(2L, "手表", 3),
        Order(2L, "笔记本", 3),
        Order(4L, "计算器", 1)
      )
    )
    //将DataStream转为Table
    val tableA: Table = tableEnv.fromDataStream(orderStreamA, $"user", $"product", $"amount")
    //将Table的schema以摘要格式打印到控制台
    tableA.printSchema()
    //将DataStream转为视图，名称为tableB
    tableEnv.createTemporaryView("tableB", orderStreamB, $("user"), $("product"), $("amount"))
    //执行SQL查询，合并查询结果
    println("tableA默认表名：" + tableA.toString)
    val resultTable: Table = tableEnv.sqlQuery(
      "SELECT * FROM " + tableA + " WHERE amount > 2 " +
        "UNION ALL " +
        "SELECT * FROM tableB WHERE amount > 2"
    )
    //将结果Table转为仅追加（Append-only）流
    val dataStreamResult = tableEnv.toAppendStream[Order](resultTable)
    //将流打印到控制台
    dataStreamResult.print()
    //触发程序执行
    env.execute()
  }
}
//创建订单样例类
case class Order(user: Long, product: String, amount: Int)