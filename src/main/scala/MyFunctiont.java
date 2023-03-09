//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.state.ReducingState;
//import org.apache.flink.api.common.state.ReducingStateDescriptor;
//import org.apache.flink.runtime.state.FunctionInitializationContext;
//import org.apache.flink.runtime.state.FunctionSnapshotContext;
//import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
//
//public class MyFunctiont<T> implements MapFunction<T, T>, CheckpointedFunction {
//    private ReducingState<Long> countPerKey;
//    private ListState<Long> countPerPartition;
//    private long localCount;
//
//    @Override
//    public T map(T value) throws Exception {
//        //更新状态
//        countPerKey.add(1L);
//        localCount++;
//        return value;
//    }
//
//    @Override
//    public void snapshotState(FunctionSnapshotContext context) throws Exception {
//        //Keyed state总是最新的，只需把每个分区的状态呈现出来即可
//        countPerPartition.clear();
//        countPerPartition.add(localCount);
//    }
//
//    @Override
//    public void initializeState(FunctionInitializationContext context) throws Exception {
//        //获取每个Key对应的状态数据结构
//        countPerKey = context.getKeyedStateStore().getReducingState(
//                new ReducingStateDescriptor<>("perKeyCount",
//                        new
//                                AddFunFunction<>(), Long.class));
//        //获取每个分区状态的状态数据结构
//        countPerPartition =
//        context.getOperatorStateStore().getListState(new ListStateDescriptor<>("perPartitionCount", Long.class));
//        //根据算子状态初始化本地计数变量
//        for (Long l : countPerPartition.get()) {
//            localCount += l;
//        }
//    }
//}