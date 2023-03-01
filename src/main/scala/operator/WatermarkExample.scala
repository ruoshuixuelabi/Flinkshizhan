package operator

import java.time.Duration

import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 计算5秒内每个信号灯通过的汽车数量，要求添加水印来解决网络延迟问题
  */
object WatermarkExample {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val stream = env.socketTextStream("localhost", 9999)
        //设置水印的生成周期，默认200毫秒
        env.getConfig.setAutoWatermarkInterval(200)

        //默认并行度是机器核心数，如果多并行度，水印时间是并行中最小的事件时间
        //设置并行度为1，便于观察计算结果
        env.setParallelism(1)

        val carStream: DataStream[CarData] = stream.map(line => {
            var arr = line.split(",")
            CarData(arr(0), arr(1).toInt, arr(2).toLong)
        })
        //生成水印，实现一个延迟3秒的固定延迟水印
        //Watermark=当前最大的事件时间-允许的最大延迟时间
        //https://zhuanlan.zhihu.com/p/158951593
        val waterCarStream=carStream.assignTimestampsAndWatermarks(
            //指定水印生成策略:周期性策略
            WatermarkStrategy.forBoundedOutOfOrderness[CarData](Duration.ofSeconds(3))//指定最大无序度，即允许的最大延迟时间
              .withTimestampAssigner(new SerializableTimestampAssigner[CarData] {
                //指定事件时间戳，即让Flink知道元素中的哪个字段是事件时间
                override def extractTimestamp(element: CarData, recordTimestamp: Long): Long = element.eventTime
            })
        )


        //设置5秒的滚动窗口
        waterCarStream
          .keyBy(_.id)
          .window(TumblingEventTimeWindows.of(Time.seconds(5)))
          .reduce(new MyReduceFunction3,new MyProcessWindowFunction3)
          .print()

        env.execute("WatermarkExample")
    }
}

/**
  * 车辆数据
  * @param id 信号灯ID
  * @param count 通过的车辆数量
  * @param time 事件时间戳
  */
case class CarData(id: String,count:Int,eventTime:Long)

//使用增量聚合函数进行预聚合,累加同一个信号灯下的车辆数量
class MyReduceFunction3() extends ReduceFunction[CarData] {
    override def reduce(c1:CarData,c2:CarData): CarData ={
        //CarData(信号灯ID，车辆总数，事件时间戳)，事件时间戳取任意值都可，此处只是占位使用
        CarData(c1.id,c1.count+c2.count,c1.eventTime)
    }
}

//使用全量聚合函数获取窗口开始和结束时间
//ReduceFunction计算完毕后，会将每组数据（以Key分组）的计算结果输入到ProcessWindowFunction的process()方法中
class MyProcessWindowFunction3() extends ProcessWindowFunction[CarData,String, String, TimeWindow] {
    //窗口结束的时候调用（每次传入一个分组的数据）
    override def process(key: String, context: Context, elements: Iterable[CarData], out: Collector[String]): Unit = {
        //从elements变量中获取聚合结果，本例该变量中只有一条数据,即聚合的总数
        val carDataReduce:CarData = elements.iterator.next()
        //输出窗口开始时间与最小值
        out.collect("窗口["+context.window.getStart.toString+"~"+context.window.getEnd+")的计算结果："+(key,carDataReduce.count))

    }

}