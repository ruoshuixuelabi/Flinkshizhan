package sql

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.hive.HiveCatalog
/**
  * Flink SQL连接Hive，分析表数据
  */
object Demo05_SQL_Hive {
    def main(args: Array[String]): Unit = {
        //1.创建表执行环境
       val settings = EnvironmentSettings
          .newInstance()
          .useBlinkPlanner()
          .inBatchMode()//批处理模式，默认为流模式
          .build()
        val tableEnv = TableEnvironment.create(settings)

        //2.连接Hive
        val catalogName= "myhive"//Catalog名称
        val defaultDatabase="default"//默认数据库名称
        //Hive配置文件目录位置
        val hiveConfDir= "/opt/modules/apache-hive-2.3.3-bin/conf/"

        val hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir)
        //注册一个Catalog，名称为"myhive"（Catalog下可以创建数据库和表）
        tableEnv.registerCatalog("myhive", hive)
        //使用已经注册的"myhive"作为默认Catalog
        tableEnv.useCatalog("myhive")

        //3.执行统计
        //切换到traffic_db数据库
        tableEnv.executeSql("use traffic_db")
        //统计正常卡口数量
        tableEnv.executeSql(
            "select " +
              "count(distinct monitor_id) as total " +
              "from monitor_flow_action "
        ).print()

        //统计车流量排名前3的卡口号
        tableEnv.executeSql(
            " select monitor_id,count(*) as total " +
              "from monitor_flow_action " +
              "group by monitor_id " +
              "order by total desc " +
              "limit 3"
        ).print()
        //统计每个卡口通过速度最快的前3辆车
        tableEnv.executeSql(
            """
            SELECT
              *
              FROM(
                  SELECT
                    monitor_id,
                  car,
                  speed,
                  row_number () over (PARTITION BY monitor_id ORDER BY cast(speed AS INT) DESC ) AS row_num
                    FROM monitor_flow_action
              ) t
              WHERE
              t.row_num <= 3
             """
        ).print()
        //车辆轨迹分析
        tableEnv.executeSql(
            """
              select
             |    monitor_id,
             |    action_time
             |from monitor_flow_action
             |where car='京S53909'
             |and action_time>='2018-04-26'
             |and action_time<='2018-04-27'
             |order by action_time asc;
            """
        ).print()

    }
}