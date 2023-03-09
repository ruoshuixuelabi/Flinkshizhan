package sql

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 统计每一个产品类别的销售额前三名（相当于分组求TOPN）
 */
object Demo06_SQL_Over_TopN {
  def main(args: Array[String]): Unit = {
    //1.创建流表执行环境
    /*val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    // 或 val tableEnv = TableEnvironment.create(settings)*/
    //1.创建批表执行环境
    val settings = EnvironmentSettings
      .newInstance()
//      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val tableEnv = TableEnvironment.create(settings)
    //2.创建数据源表，读取文件数据
    //字段：日期，产品类别，销售额
    tableEnv.executeSql(
      """
            CREATE TABLE t_sales(
                `date` STRING,
                `type` STRING,
                 money INT
            ) WITH (
              'connector' = 'filesystem',
              'path' = 'D:\gongzuo\ziji\Flinkshizhan\src\main\scala\data\sales.csv',
              'format' = 'csv'
            )
            """
    )
    //3.执行TopN查询，并打印结果
    tableEnv.executeSql(
      """
            select `date`,`type`,money,rownum
            from(
                select `date`,`type`,money,
                    row_number() over (partition by `type` order by money desc) rownum
                from t_sales) t
            where t.rownum<=3
            """
    ).print()
  }
}