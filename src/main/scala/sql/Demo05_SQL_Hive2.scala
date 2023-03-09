package sql

import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{$, EnvironmentSettings}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row
/**
  * Flink SQL连接Hive，读取表数据
  */
object Demo05_SQL_Hive2 {
    def main(args: Array[String]): Unit = {
        //1.创建表执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val settings = EnvironmentSettings
          .newInstance()
//          .useBlinkPlanner()
          .inStreamingMode()
          .build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        //2.连接Hive
        val catalogName= "myhive"//Catalog名称
        val defaultDatabase="traffic_db"//默认数据库名称
        //Hive配置文件目录位置
        val hiveConfDir= "/opt/modules/apache-hive-2.3.3-bin/conf/"

        val hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir)
        //注册一个Catalog，名称为"myhive"，Catalog下可以创建数据库和表
        tableEnv.registerCatalog("myhive", hive)
        //使用已经注册的"myhive"作为默认Catalog
        tableEnv.useCatalog("myhive")

        //3.执行SQL
        //        val result = tableEnv.executeSql(
        //            "insert into user_info values(3,'wangwu')"
        //        )
        val resultTable:Table=tableEnv.sqlQuery(" select count(distinct monitor_id) from monitor_flow_action where `date`>='2018-04-26' and `date`<='2018-04-27'")
        //结果表转为流
        val resultStream=tableEnv.toRetractStream[Row](resultTable)
        //将流打印到控制台
        resultStream.print()




    }
}