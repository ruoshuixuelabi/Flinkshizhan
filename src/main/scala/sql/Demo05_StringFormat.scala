package sql

object Demo05_StringFormat {
    def main(args: Array[String]): Unit = {
        //Scala多行字符串
        println(
         """
           |select
           | user_id,
           | product_id,
           | status
           |from input_table
           |where status = 'success'
          """.stripMargin
        )
    }

}
