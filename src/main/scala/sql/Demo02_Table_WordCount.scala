package sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
  * Flink Table API或SQL实时单词计数
  */
object Demo02_Table_WordCount {

    def main(args: Array[String]): Unit = {
        //1.创建表执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val settings = EnvironmentSettings
          .newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build()
        val tableEnv = StreamTableEnvironment.create(env, settings)

        //2.读取文件作为输入流，进行简单数据类型处理
        val inputStream: DataStream[String] = env.readTextFile(
            "D:\\input\\words.txt"
        )
        //val inputStream=env.socketTextStream("localhost",9999)
        //将单词转为(单词,1)的形式
        val dataStream: DataStream[(String, Int)] = inputStream
          .flatMap(_.split(" ")) //按照空格分割单词
          .map((_, 1))

        //3.数据流DataStream转化成Table表，并指定相应字段
        val inputTable:Table = tableEnv.fromDataStream[(String, Int)](
            dataStream,
            $"word",
            $"count"
        )
        //4.使用Table API对Table表进行关系操作
        val resultTable:Table = inputTable
          .groupBy($"word")
          .select($"word", $"count".sum)

        //或者使用SQL API
        /*val resultTable:Table = tableEnv.sqlQuery(
            s"""
              |select
              | word,
              | count(1) as wordcount
              |from $inputTable
              |group by word
            """.stripMargin
        )*/
        //5.结果Table转成DataStream数据流，并输出到控制台
        resultTable.toRetractStream[(String,Long)].print()
        //或者
        //resultTable.toRetractStream[Row].print()
        //6.任务执行
        env.execute("MyJob")
    }
}
