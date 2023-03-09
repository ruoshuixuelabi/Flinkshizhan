package sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * Flink SQL实时读取Kafka数据，处理后写入Kafka
 */
object Demo04_SQL_Kafka {
  def main(args: Array[String]): Unit = {
    //1.创建表执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings
      .newInstance()
//      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    //2.创建Kafka Source
    tableEnv.executeSql(
      "CREATE TABLE input_table (" +
        "  `user_id` BIGINT," +
        "  `product_id` BIGINT," +
        "  `status` STRING" +
        ") WITH (" +
        "  'connector' = 'kafka'," +
        "  'topic' = 'topic01'," +
        "  'properties.bootstrap.servers' = 'centos01:9092,centos02:9092,centos03:9092'," +
        "  'properties.group.id' = 'testGroup'," +
        "  'scan.startup.mode' = 'latest-offset'," +
        "  'format' = 'json'" +
        ")"
    )
    //3.执行查询：查询订单状态为“success”的数据
    val inputTable: Table = tableEnv.sqlQuery("" +
      "select " +
      "user_id," +
      "product_id," +
      "status " +
      "from input_table " +
      "where status = 'success'"
    )
    //将查询结果Table转为流，输出到控制台
    inputTable.toRetractStream[Row].print()
    //4.创建Kafka Sink
    tableEnv.executeSql(
      "CREATE TABLE output_table (" +
        "  `user_id` BIGINT," +
        "  `product_id` BIGINT," +
        "  `status` STRING" +
        ") WITH (" +
        "  'connector' = 'kafka'," +
        "  'topic' = 'topic02'," +
        "  'properties.bootstrap.servers' = 'centos01:9092,centos02:9092,centos03:9092'," +
        "  'format' = 'json'," +
        "  'sink.partitioner' = 'round-robin'" +
        ")"
    )
    //5.向Kafka Sink写入数据
    tableEnv.executeSql("insert into output_table select * from " + inputTable)
    //6.触发执行
    env.execute("MyJob")
  }
}