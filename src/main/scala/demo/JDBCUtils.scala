package demo
import java.sql.{Connection, DriverManager}
/**
  * JDBC工具类
  */
object JDBCUtils {
    private val driver = "com.mysql.jdbc.Driver"
    private val url = "jdbc:mysql://localhost:3306/student_db"
    private val username = "root"
    private val password = "123456"
    /**
      * 获得数据库连接
      */
    def getConnection(): Connection = {
        Class.forName(driver)
        val conn = DriverManager.getConnection(url, username, password)
        conn
    }
}