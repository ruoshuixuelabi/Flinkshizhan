package partition

import org.apache.flink.api.common.functions.Partitioner
/**
  * 自定义分区策略（分区器），实现对Key分区的自定义分配
  */
class MyPartitioner extends Partitioner[String]{
    /**
      * 根据给定的Key计算分区
      * @param key 分区的Key
      * @param numPartitions 分区数量，根据下游算子的并行度指定
      * @return 分区索引，相当于分区编号。如果numPartitions为3，则索引取值为0,1,2
      */
    override def partition(key: String, numPartitions: Int): Int = {
        if(key.equals("chinese")){//将key值为chinese的数据分到0号分区
            0
        }else if(key.equals("math")){//将key值为math的数据分到1号分区
            1
        }else{//其余数据分到2号分区
            2
        }
    }
}