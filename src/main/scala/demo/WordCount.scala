package demo

import org.apache.flink.api.scala._

/**
 * 单词计数批处理
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //第一步：创建批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //第二步：读取本地数据，并转为DataSet数据集
    val inputDataSet: DataSet[String] = env.readTextFile("D:\\gongzuo\\ziji\\Flinkshizhan\\src\\main\\scala\\data\\words.txt")
    //第三步：转换数据
    //将单词按照空格进行分割并合并为一个新的DataSet数据集
    val wordDataSet: DataSet[String] = inputDataSet.flatMap(_.split(" "))
    //将数据集的每个元素转为(单词,1)形式的元组
    val tupleDataSet: DataSet[(String, Int)] = wordDataSet.map((_, 1))
    //将数据集按照元素中的第一个字段重新分区
    val groupedDataSet: GroupedDataSet[(String, Int)] = tupleDataSet.groupBy(0)
    //对相同key值（单词）下第二个字段进行求和运算
    val resultDataSet: DataSet[(String, Int)] = groupedDataSet.sum(1)
    //第四步：打印结果到控制台
    resultDataSet.print()

    //或者
    //对每一组数据聚合，每一组数据将传入到group中,group类型：Iterator(String, Int)
    /*groupedDataSet.reduceGroup(group=>{
        var count=0
        var word=""
        while(group.hasNext){
            val line=group.next()
            count+=line._2.toInt
            word=line._1.toString
        }
        (word,count)
    }).print()*/
  }
}