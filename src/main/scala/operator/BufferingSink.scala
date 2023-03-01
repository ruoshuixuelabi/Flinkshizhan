package operator

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import scala.collection.mutable.ListBuffer

class BufferingSink(threshold: Int = 0) extends SinkFunction[(String, Int)]
  with CheckpointedFunction {

    //Operator List State
    private var checkpointedState: ListState[(String, Int)] = _
    //本地缓存
    private val bufferedElements = ListBuffer[(String, Int)]()

    /**
      * Sink的核心处理逻辑，将给定的值写入Sink。每个记录都会调用该函数
      * @param value 给定的值
      * @param context 上下文对象
      */
    override def invoke(value: (String, Int), context: Context):Unit ={
        //先将数据缓存到本地的缓存
        bufferedElements += value
        //当本地缓存大小达到阈值时，将本地缓存输出到外部系统
        if (bufferedElements.size == threshold) {
            for (element <- bufferedElements) {
                //输出到外部系统（需要实现）
                //…
            }
            //清空本地缓存
            bufferedElements.clear()
        }
    }
    /**
      * 当进行Checkpoint时，将调用此方法
      */
    override def snapshotState(context: FunctionSnapshotContext): Unit ={
        //清除状态
        checkpointedState.clear()
        //将本地缓存添加到ListState
        for (element <- bufferedElements) {
            checkpointedState.add(element)
        }
    }
    /**
      * 算子子任务初始化时调用此方法
      */
    override def initializeState(context: FunctionInitializationContext): Unit = {
        //创建状态描述器，描述状态名称、数据类型等，用于在有状态算子中创建可分区的状态
        val descriptor = new ListStateDescriptor[(String, Int)](
            "buffered-elements",
            TypeInformation.of(new TypeHint[(String, Int)]() {})
        )
        //创建ListState，每个ListState都使用唯一的名称
        checkpointedState = context
          .getOperatorStateStore.getListState(descriptor)
        //如果从前一个执行的快照恢复状态，则返回true（例如作业重启的情况）
        if (context.isRestored) {
            //读取存储中的状态数据并填充到本地缓存中
            /*for (element <- checkpointedState.get()) {
                bufferedElements += element
            }*/
        }
    }
}
