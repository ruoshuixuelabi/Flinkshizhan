package operator

import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.api.common.functions.ReduceFunction
object ReduceFunctionExample {

    def main(args: Array[String]): Unit = {

        //创建流处理的执行环境
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        //创建DataStream集合
        val dataStream: DataStream[Score] = senv.fromElements(
            Score("Zhang", "English", 98),//姓名，科目，成绩
            Score("Li", "English", 88),
            Score("Zhang", "Math", 78),
            Score("Li", "Math", 79)
        )
        //自定义Reduce函数
        class MyReduceFunction() extends ReduceFunction[Score] {
            //接收两个参数输入，输出聚合后的值
            override def reduce(s1: Score, s2: Score): Score = {
                //输出的元素数据类型应与输入的参数类型保持一致
                Score(s1.name, "Sum", s1.score + s2.score)
            }
        }

        val reducedDataStream = dataStream
          .keyBy(_.name) //根据姓名分组
          .reduce((s1, s2) => {
            Score(s1.name, "Sum", s1.score + s2.score)
          })

        /*val reducedDataStream=dataStream
          .keyBy(_.name)
          .reduce(new MyReduceFunction)*/

        //打印聚合结果到控制台
        reducedDataStream.print()
        //执行作业，指定作业名称
        senv.execute("StreamReduce")
    }
}
//创建成绩样例类
case class Score(name: String, course: String, score: Int)