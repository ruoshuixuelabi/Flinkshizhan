package sql

import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

import scala.util.Random

/**
 * Flink SQL实时计算5秒内每个用户的订单总数、订单总金额
 */
object Demo03_SQL_Window {
  def main(args: Array[String]): Unit = {
    //1.创建表执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings
      .newInstance()
//      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    //2.模拟实时流数据（使用自定义数据源）
    val orderDataStream: DataStream[OrderA] = env.addSource(new
        RichSourceFunction[OrderA] {
      private var isRunning = true

      //启动Source
      override def run(ctx: SourceFunction.SourceContext[OrderA]): Unit = {
        val random = new Random()
        while (isRunning) {
          //用户订单数据
          val order = OrderA(
            UUID.randomUUID().toString(), //订单ID
            random.nextInt(5), //用户ID：随机数0~4
            random.nextInt(200), //订单金额：随机数0~199
            System.currentTimeMillis() //订单时间：当前时间毫秒数
          )
          TimeUnit.SECONDS.sleep(1) //线程睡眠1秒钟
          ctx.collect(order) //发射一个订单元素
        }
      }

      override def cancel(): Unit = isRunning = false
    })
    //3.生成水印，实现一个延迟3秒的固定延迟水印
    //水印Watermark=当前最大的事件时间-允许的最大延迟时间
    val waterOrderStream = orderDataStream.assignTimestampsAndWatermarks(
      //指定水印生成策略:周期性策略
      WatermarkStrategy.forBoundedOutOfOrderness[OrderA](Duration
        .ofSeconds(3)) //指定最大无序度，即允许的最大延迟时间
        .withTimestampAssigner(new SerializableTimestampAssigner[OrderA] {
          //指定事件时间戳，即让Flink知道元素中的哪个字段是事件时间
          override def extractTimestamp(element: OrderA, recordTimestamp: Long): Long = element.createTime
        })
    )

    //4.注册一张表（视图）
    tableEnv.createTemporaryView("t_order", waterOrderStream,
      $("orderId"),
      $("userId"),
      $("money"),
      $("createTime").rowtime()
    )
    //5.执行SQL查询
    val sql = "select " +
      "userId," +
      "count(*) as totalCount," +
      "sum(money) as sumMoney " +
      "from t_order " +
      "group by userId," +
      "tumble(createTime, interval '5' second)" //5秒滚动窗口
    val resultTable: Table = tableEnv.sqlQuery(sql)
    //6.结果Table转为DataStream
    val resultStream = tableEnv.toRetractStream[Row](resultTable)
    //或
    //val resultStream: DataStream[(Boolean, (Int, Long, Double))] = tableEnv.toRetractStream[(Int,Long,Double)](resultTable)
    //或
    //resultTable.toRetractStream[Row].print()
    //7.输出结果
    resultStream.print()
    //8.触发执行
    env.execute("MyJob")
  }
}

//样例类
case class OrderA(orderId: String, userId: Int, money: Double, createTime: Long)