package demo

import java.sql.{Connection, PreparedStatement}

import flink.demo.Domain.Student
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  * 自定义MySQL数据源
  */
class MySQLSource extends RichSourceFunction[Student] {

    var conn: Connection = _//数据库连接对象
    var ps: PreparedStatement = _//SQL命令执行对象
    var isRunning=true//是否运行（是否持续从数据源读取数据）

    /**
      * 初始化方法
      * @param parameters 存储键/值对的轻量级配置对象
      */
    override def open(parameters: Configuration): Unit = {
        //获得数据库连接
        conn = JDBCUtils.getConnection
        //获得命令执行对象
        ps = conn.prepareStatement("select * from student")
    }

    /**
      * 当开始从数据源读取元素时，该方法将被调用
      * @param ctx 用于从数据源发射元素
      */
    override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
        //执行查询
        val rs = ps.executeQuery()
        //循环读取集合中的数据并发射出去
        while (isRunning&&rs.next()) {
            val student = Student(
                rs.getInt("id"),
                rs.getString("name"),
                rs.getInt("age")
            )
            //从数据源收集一个元素数据并发射出去，而不附加时间戳（默认方式）
            ctx.collect(student)
        }
    }

    /**
      * 取消数据源读取。
      * 大多数自定义数据源在run()方法中会有while循环，需要确保调用该方法后跳出run()方法中的while循环。
      * 典型的解决方法可以使用布尔类型的变量isRunning，在该方法中将变量置为false，在while循环中检查该变量
      * 当数据源读取操作被取消时，执行线程也会被中断。中断严格地发生在此方法被调用之后
      */
    override def cancel(): Unit = {
        this.isRunning=false
    }
}