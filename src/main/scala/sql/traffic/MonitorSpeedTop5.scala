package sql.traffic

import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{$, EnvironmentSettings}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
/**
  * Flink SQL连接Hive，分析表数据
  */
object MonitorSpeedTop5 {
    def main(args: Array[String]): Unit = {
        //创建流执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //创建EnvironmentSettings实例并设置参数
        val settings = EnvironmentSettings
          .newInstance()//创建一个用于创建EnvironmentSettings实例的构建器
//          .useBlinkPlanner()//将Blink计划器设置为所需的模块（默认）
          .inStreamingMode()//设置组件以流模式工作。默认启用
          .build()//创建一个不可变的EnvironmentSettings实例
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

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
        val monitorFlowTable:Table=tableEnv.sqlQuery("select * from monitor_flow_action ")

//        tableEnv.toDataStream(monitorFlowTable).print()

        val monitorFlowDataStream:DataStream[Row]=tableEnv.toAppendStream[Row](monitorFlowTable)
        val monitorFlowKV: DataStream[(String, Row)] = monitorFlowDataStream.map(line => {
            //(monitor_id,Row)
            (line.get(1).toString, line)
        })
        val groupedStream: KeyedStream[(String, Row), String] = monitorFlowKV.keyBy(_._1)
        /*groupedStream.process(new KeyedProcessFunction[String,(String, Row)] {
            //统计各类速度的车辆数量
            var lowSpeedCount = 0
            var normalSpeedCount = 0
            var mediumSpeedCount = 0
            var highSpeedCount = 0
            override def processElement(value: (String, Row),
                                        ctx: KeyedProcessFunction[String, (String, Row), (SpeedSortKey,String)]#Context,
                                        out: Collector[(SpeedSortKey,String)]): Unit = {
                val speed=value._2.get(5).toString.toInt
                if (speed >= 0 && speed < 60)
                    lowSpeedCount += 1
                else if (speed >= 60 && speed < 90)
                    normalSpeedCount += 1
                else if (speed >= 90 && speed < 120)
                    mediumSpeedCount += 1
                else if (speed >= 120)
                    highSpeedCount += 1
            }

//            (speedSortKey, monitorId)

            def onTimer(timestamp: Long,
                        ctx: KeyedProcessFunction[String, (String, Row), (SpeedSortKey,String)]#Context,
                        out: Collector[(SpeedSortKey,String)]){

                //将各类速度的车辆数量存入自定义排序类SpeedSortKey
                val speedSortKey = new SpeedSortKey(lowSpeedCount, normalSpeedCount,
                    mediumSpeedCount, highSpeedCount)
            }

        })*/

//https://www.cnblogs.com/bolingcavalry/p/14014561.html

    }
}