package operator

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * 整合Kafka实现单词计数流处理
 */
object StreamKafkaWordCount {
  def main(args: Array[String]): Unit = {
    //第一步：创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //第二步：从Kafka读取流数据
    val properties = new Properties()
    //Kafka Broker连接地址
    properties.setProperty("bootstrap.servers", "centos01:9092,centos02:9092,centos03:9092")
    //Kafka消费者组ID
    properties.setProperty("group.id", "test")
    //添加数据源
    val stream = env.addSource(new FlinkKafkaConsumer[String](
      "topictest", //Kafka主题名称
      new SimpleStringSchema(), //字符串反序列化器
      properties)) //Kafka连接属性
    //第三步：转换Kafka流数据
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .filter(_.nonEmpty) //过滤空字段
      .map((_, 1)) //转换成（单词,1）形式的元组
      .keyBy(_._1) //按照key（元组中第一个值）对数据重分区
      .sum(1) //执行求和运算
    //第四步：输出结果到控制台
    result.print()
    //第五步：触发任务执行
    env.execute("StreamKafkaWordCount") //指定作业名称
  }
}