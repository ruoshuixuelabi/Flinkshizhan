package operator

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Demo01StateBackend {
    def main(args: Array[String]): Unit = {
        val env=StreamExecutionEnvironment.getExecutionEnvironment
        //设置FsStateBackend状态后端
        env.setStateBackend(new HashMapStateBackend)
        env.getCheckpointConfig.setCheckpointStorage("hdfs://checkpoints")

        //设置MemoryStateBackend状态后端
        env.setStateBackend(new HashMapStateBackend)
        env.getCheckpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage)

        //设置RocksDBStateBackend状态后端
        env.setStateBackend(new EmbeddedRocksDBStateBackend)
        env.getCheckpointConfig.setCheckpointStorage("hdfs://checkpoints")
    }

}
