package org.fdh.day04;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.fdh.bean.stock.StockPrice;
import org.fdh.bean.stock.StockPriceSource;

public class CheckpointExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint，设置间隔为1000ms
        env.enableCheckpointing(1000);
        // 设置模式为精确一次 (默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 创建基于HashMap的状态后端
        StateBackend stateBackend = new HashMapStateBackend();
        // 配置检查点存储位置
        env.setStateBackend(stateBackend);
        CheckpointStorage checkpointStorage = new FileSystemCheckpointStorage("file:///e:/backend");
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);
//        env.setStateBackend(new FsStateBackend("file:///e:/backend"));
        //这行代码设置了作业在取消时checkpoint的行为。通过使用RETAIN_ON_CANCELLATION策略，当作业被取消时，外部化的checkpoint将被保留，
        //而不是删除。这意味着你可以在作业取消后手动恢复到这些保留的checkpoint状态。这对于调试和故障排除非常有用。
        //如果你选择DELETE_ON_CANCELLATION，那么作业取消时checkpoint会被删除。
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<Tuple2<String, Double>> stream = env.addSource(new StockPriceSource("stock/stock-tick-20200108.csv"), "stock")
                .keyBy(StockPrice::getSymbol)
                .map(value -> Tuple2.of(value.getSymbol(), value.getPrice() * 2))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));
        // 打印结果
        stream.print();
        // 执行程序
        env.execute("Checkpoint Example");
    }
}
