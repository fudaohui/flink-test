package org.fdh.day05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class CheckPointSourceExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
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
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = env.addSource(new CheckPointSource())
                .map(new FailingMapFunction(20));
        stream.print();
        env.execute("checkpoint-fail-job");
    }
}
