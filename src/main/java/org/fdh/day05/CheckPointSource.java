package org.fdh.day05;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * 1. checkpoint用于flink系统的容错功能，确保发生故障的时候能从一个稳定的状态（快照）中恢复，并提供精确一致的语音（通常需要数据源支持消息重复和offset功能）
 * 2. RichSourceFunction 相比SourceFunction多了open方法用来初始化资源（如初始化数据库连接），close关闭资源（关闭数据库连接），获取运行时上下文的功能
 * 3、flink中的状态分为托管状态和raw状态，托管状态分为键控状态(Keyed State,状态和具体的keyby建相关)和算子状态(operator state，和并行的算子总实例相关，即各子任务共享)。
 *
 * 具体步骤：
 * Flink是一个分布式流计算处理引擎，为了解决分布式处理引擎可能面临的故障问题，保证在发生故障时能够完好的恢复，并且不丢失状态数据，Flink定义了checkpoint机制。
 * 在checkpoint机制中，当所有任务处理完所有的等量的原始输入后，会对全部任务状态进行一个拷贝。其执行过程大致如下：
 * 暂停接收所有输入流。
 * 等待处理完已经流入系统的数据，生成最新的状态。
 * 将所有任务的状态拷贝到远程持久化存储，在所有任务完成拷贝后，生成全局一致性检查点。
 * 恢复所有算子数据流的输入。
 * 此外，为了保证state的容错性，Flink需要对state进行checkpoint。Checkpoint表示了一个Flink Job在一个特定时刻的一份全局状态快照，即包含了所有task/operator的状态。
 * 因此，当Flink程序意外崩溃时，重新运行程序时可以有选择地从这些快照进行恢复，从而修正因为故障带来的程序数据异常。
 * <p>
 * 下面代码让source支持checkpoint,即使遇到故障也能从最近的checkpoint中恢复，且支持了仅有一次source的效果
 */
public class CheckPointSource extends RichSourceFunction<Tuple2<String, Integer>> implements CheckpointedFunction {


    private int offset = 0;
    private boolean isRunning = true;
    private ListState<Integer> offsetState;

    /*Flink定期保存状态数据到存储上，故障发生后从之前的备份中恢复，整个过程被称为Checkpoint机制，它为Flink提供了Exactly-Once的投递保障。该机制具体流程如下：
        暂停处理新流入数据，将新数据缓存起来。
        将算子子任务的本地状态数据拷贝到一个远程的持久化存储上。
        继续处理新流入的数据，包括刚才缓存起来的数据。
        Flink的Checkpoint机制可以周期性执行，一般由JobManager checkpoint coordinator触发*/
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        offsetState.add(offset);
    }

    /***
     * 第一次初始化或从故障恢复中初始化state状态
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Integer> desc = new ListStateDescriptor<>("offset", Types.INT);
        offsetState = context.getOperatorStateStore().getListState(desc);
        Iterable<Integer> state = offsetState.get();
        //第一次初始化从0开始计数
        if (state == null || !state.iterator().hasNext()) {
            offset = 0;
        } else {
            offset = state.iterator().next();
        }
    }

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {

        while (isRunning) {
            Thread.sleep(200);
            //但在进行checkpoint时候数据停发
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(Tuple2.of(offset + "", 1));
                offset++;
            }
            if (offset == 1000) {
                isRunning = false;
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
