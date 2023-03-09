package operator

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.environment.CheckpointConfig

object CheckpointExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //流程序的运行时执行模式。除此之外，它还控制任务调度、网络shuffle行为和时间语义。一些操作还将根据配置的执行模式更改其记录发送行为。
    //运行模式：批、流、自动
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    env.setParallelism(1) //设置并行度为1，方便观察结果
    env.enableCheckpointing(1000) //每隔1秒执行一次Checkpoint
    //指定状态后端
    //    env.setStateBackend(new FsStateBackend("file:///D:checkpoint"))
    //上面方法已经废弃,推荐下面的写法
    env.setStateBackend(new HashMapStateBackend());
    env.getCheckpointConfig.setCheckpointStorage("file:///D:checkpoint");
    //设置模式为精确一次(默认值)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置两次Checkpoint之间的最小时间间隔为500毫秒。
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //设置可容忍的失败的Checkpoint数量，默认值为0，意味着不容忍任何Checkpoint失败
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(10)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointTimeout(6000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //重启策略：固定延迟重启。程序出现异常的时候重启2次，每次时间间隔3秒，超过2次程序仍然出现异常则退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000))
  }
}