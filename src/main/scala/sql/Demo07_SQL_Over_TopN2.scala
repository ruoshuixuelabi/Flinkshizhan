package sql

import org.apache.flink.table.api.{EnvironmentSettings, _}
/**
  * 统计每天搜索数量最多的搜索词前3名
  * （同一天中同一用户多次搜索同一个搜索词视为1次）
  */
object Demo07_SQL_Over_TopN2 {
    def main(args: Array[String]): Unit = {
        //1.创建批表执行环境
        val settings = EnvironmentSettings
          .newInstance()
          .useBlinkPlanner()
          .inBatchMode()
          .build()
        val tableEnv = TableEnvironment.create(settings)

        //2.创建数据源表，读取keywords.csv文件数据
        //字段：日期，用户ID，搜索词
        tableEnv.executeSql(
            """
            CREATE TABLE user_keywords(
                `date` STRING,
                 user_id STRING,
                 keyword STRING
            ) WITH (
              'connector' = 'filesystem',
              'path' = 'D:\document\idea-workspace\MyFlinkDemo\src\main\scala\data\keywords.csv',
              'format' = 'csv'
            )
            """
        )

        //3.执行查询
        //根据(日期,关键词)进行分组，获取每天每个搜索词被哪些用户进行了搜索
        //同时对用户ID进行去重，并统计去重后的数量,获得其uv
        val uvTable=tableEnv.sqlQuery(
            """
              |SELECT
              |    `date`,keyword,COUNT(DISTINCT user_id) as uv
              |FROM user_keywords
              |GROUP BY `date`,keyword
            """.stripMargin
        )

        //4.执行TopN查询，并打印结果
        //统计每天搜索uv排名前3的搜索词
        tableEnv.executeSql(
            s"""
            SELECT `date`,keyword,uv
            from(
                select `date`,keyword,uv,
                    row_number() over (partition by `date` order by uv desc) rownum
                from ${uvTable}) t
            where t.rownum<=3
            """
        ).print()

    }
}