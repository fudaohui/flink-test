package org.fdh.day05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;

public class TwoPhaseFileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple2<String, Integer>> stream = env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
                .addSource(new CheckPointSource())
//                .keyBy(o -> o.f0)
                .map(new FailingMapFunction(20));


        // 类Unix系统的临时文件夹在/tmp下
        // Windows用户需要修改这个目录
        String preCommitPath = "E:\\backend\\flink-sink-precommited\\";
        String commitedPath = "E:\\backend\\flink-sink-commited\\";

        if (!Files.exists(Paths.get(preCommitPath))) {
            Files.createDirectory(Paths.get(preCommitPath));
        }
        if (!Files.exists(Paths.get(commitedPath))) {
            Files.createDirectory(Paths.get(commitedPath));
        }
        // 使用Exactly-Once语义的Sink，执行本程序时可以查看相应的输出目录，查看数据
        stream.addSink(new TwoPhaseFileSink(preCommitPath, commitedPath));
        // 数据打印到屏幕上，无Exactly-Once保障，有数据重发现象
        stream.print();
        env.execute("two file sink");
    }
}
