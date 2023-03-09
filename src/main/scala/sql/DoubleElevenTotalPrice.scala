package sql

import java.text.SimpleDateFormat
import java.util
import java.util.{Comparator, PriorityQueue}
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

import org.apache.commons.collections.IteratorUtils
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 天猫双十一实时交易大屏统计
 * 需求如下：
 * -1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额
 * -2.计算出销售额top3的分类
 * -3.每1秒钟更新一次统计结果
 * */
object DoubleElevenTotalPrice {
  def main(args: Array[String]): Unit = {
    //1.创建表执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2.模拟实时流数据（使用自定义数据源）
    //(订单分类,订单金额)
    val orderDataStream: DataStream[(String, Double)] = env.addSource(new MyDataSource)
    //        orderDataStream.print()
    //每隔1秒计算一次当天00:00:00截止到当前时间各个分类的订单总额
    val initResultDS = orderDataStream
      .keyBy(_._1)
      .window(
        //从当天00:00:00开始计算当天的数据。窗口开始时间、结束时间一直不变，结束时间一直为第二天零点
        //例如当前触发计算的时间为：2021-11-11 16:39:22,则窗口结束时间：2021-11-12 00:00:00
        /*窗口开始时间：2021-11-11 00:00:00，窗口结束时间：2021-11-12 00:00:00*/
        TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8))
      ).trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1))) //ContinuousProcessingTimeTrigger可以根据指定的时间间隔不断的触发。此处指在窗口时间范围内，每隔1秒触发一次计算
      .aggregate(new MyPriceAggregateFunction, new MyWindowFunction) //自定义聚合和结果收集(PriceAggregate聚合的结果将输入到MyWindowFunction中)
    initResultDS.print("分类销售总额")

    //继续处理
    initResultDS
      .keyBy(_.dateTime)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .process(new MyFinalProcessWindowFunction()) //window后的process方法可以处理复杂逻辑

    //8.触发执行
    env.execute("MyJob")

  }

}

/**
 * 自定义数据源，实时产生订单数据：(订单所属分类, 订单金额）
 */
class MyDataSource extends RichSourceFunction[(String, Double)] {
  private var isRunning = true
  //商品分类
  private val categorys = Array("女装", "男装", "图书", "家电", "洗护",
    "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公")

  //启动Source
  override def run(ctx: SourceFunction.SourceContext[(String, Double)]): Unit = {
    val random = new Random()
    while (isRunning) {
      //获取随机分类名称
      val index: Int = random.nextInt(categorys.length) //随机数范围：[0,categorys.length-1]
      val category: String = categorys(index)
      //产生订单金额。nextDouble生成的是[0~1)之间的随机数,*100之后表示[0~100)
      val price: Double = random.nextDouble * 100
      val orderPrice = price.formatted("%.2f").toDouble //保留两位小数
      TimeUnit.MILLISECONDS.sleep(20) //线程睡眠20毫秒
      ctx.collect((category, orderPrice)) //发射一个元素: (订单分类,订单金额)
    }
  }

  //取消源，该方法需要确保在调用此方法后，run()中的循环将跳出
  override def cancel(): Unit = isRunning = false
}


/**
 * 自定义订单金额聚合函数,对每一个分类的所有订单金额求和
 * AggregateFunction<输入元素类型, 累加器（中间聚合状态）类型, 输出元素类型>
 */
class MyPriceAggregateFunction extends AggregateFunction[(String, Double), Double, Double] {
  /**
   * 创建累加器，初始值为0
   */
  override def createAccumulator(): Double = 0D

  /**
   * 将窗口中的元素添加到累加器
   *
   * @param value       窗口中的元素
   * @param accumulator 累加器：金额总和
   * @return 累加器
   */
  override def add(value: (String, Double), accumulator: Double): Double = {
    value._2 + accumulator
  }

  /**
   * 获取累加结果：金额总和
   *
   * @param accumulator 累加器
   * @return 累加器：金额总和
   */
  override def getResult(accumulator: Double): Double = accumulator

  /**
   * 合并累加器，只有会话窗口才使用
   *
   * @param a 要合并的累加器
   * @param b 要合并的另一个累加器
   * @return 新累加器
   */
  override def merge(a: Double, b: Double): Double = a + b
}

/**
 * 自定义窗口函数WindowFunction,实现收集结果数据
 * WindowFunction[输入元素类型, 输出元素类型, Key类型, 窗口]
 */
class MyWindowFunction extends WindowFunction[Double, CategoryPojo, String, TimeWindow] {
  /**
   * MyPriceAggregateFunction的聚合结果将输入到该方法，结果有多个Key，因此每个Key调用一次
   *
   * @param key    分类名称，即MyPriceAggregate聚合对应的每一组的Key
   * @param window 时间窗口
   * @param input  上一个函数（MyPriceAggregateFunction）每一个Key对应的聚合结果
   * @param out    收集器，收集记录并发射出去
   */
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Double],
                     out: Collector[CategoryPojo]): Unit = {
    //分类总额，input中只有一个值
    val totalPrice: Double = input.iterator.next
    //保留两位小数
    val roundPrice = totalPrice.formatted("%.2f").toDouble
    //格式化当前系统时间为String
    val currentTimeMillis = System.currentTimeMillis
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateTime = df.format(currentTimeMillis)
    //收集并发射结果(分类名称,分类总额,该分类处理时的当前系统时间)
    out.collect(CategoryPojo(key, roundPrice, dateTime))
  }
}

/**
 * 定义全量聚合窗口处理类,收集结果数据
 * ProcessWindowFunction[输入值类型, 输出值类型, Key类型, 窗口]
 */
class MyFinalProcessWindowFunction()
  extends ProcessWindowFunction[CategoryPojo, Object, String, TimeWindow] {
  /**
   * 窗口结束的时候调用（每次传入一个分组的数据）
   *
   * @param key      CategoryPojo中的dataTime
   * @param context  窗口上下文
   * @param elements 某个Key对应的CategoryPojo集合
   * @param out      收集计算结果并输出
   */
  override def process(key: String,
                       context: Context,
                       elements: Iterable[CategoryPojo],
                       out: Collector[Object]): Unit = {

    var allTotalPrice: Double = 0D //全站的总销售额
    //PriorityQueue（优先队列），一个基于优先级堆的无界优先级队列
    //实现一个容量为3的小顶堆，其元素按照CategoryPojo的totalPrice升序排列
    val queue = new PriorityQueue[CategoryPojo](3, new Comparator[CategoryPojo] {
      //升序排列
      def compare(o1: CategoryPojo, o2: CategoryPojo): Int = {
        if (o1.totalPrice >= o2.totalPrice)
          1
        else
          -1
      }
    })

    //1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额
    for (element <- elements) {
      //把之前聚合的各个分类的销售额加起来，就是全站的总销量额
      val price = element.totalPrice //某个分类的总销售额
      allTotalPrice += price

      //2.计算出各个分类的销售额top3,其实就是对各个分类的销售额进行排序取前3
      if (queue.size < 3) { //小顶堆size<3,说明数不够,直接放入
        queue.add(element)
      } else { //小顶堆size=3,说明小顶堆满了,进来的新元素需要与堆顶元素比较
        val top: CategoryPojo = queue.peek //得到顶上的元素
        if (element.totalPrice > top.totalPrice) {
          queue.poll //移除堆顶的元素，或者queue.remove(top);
          queue.add(element) //添加新元素，会进行升序排列
        }
      }
    }

    //对queue中的数据降序排列（原来是升序，改为降序）
    val pojoes = queue.stream().sorted(new Comparator[CategoryPojo] {
      //降序排列
      def compare(o1: CategoryPojo, o2: CategoryPojo): Int = {
        if (o1.totalPrice >= o2.totalPrice)
          -1
        else
          1
      }
    })

    //转为List集合
    //        val list=IteratorUtils.toList(pojoes)
    /*//循环迭代
    while(pojoes.hasNext){
        val p=pojoes.next()
        println("分类："+p.category+"的销售总额："+p.totalPrice)
    }*/
    //3.发射统计结果，此处直接打印
    println("时间：" + key +
      "总交易额:" + allTotalPrice.formatted("%.2f") +
      "\ntop3分类:\n" + StringUtils.join(pojoes.toArray, "\n"))
    println("-------------")
  }
}

/**
 * 存储聚合的结果
 *
 * @param category   分类名称
 * @param totalPrice 该分类总销售额
 * @param dateTime   系统对该分类的统计时间
 */
case class CategoryPojo(category: String, totalPrice: Double, dateTime: String)