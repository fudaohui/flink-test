package org.fdh.day05;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于两阶段提交实现提供端到端的Exactly-Once保障（另外一种保障技术是预写日志 Write-Ahead-Log）
 * 在 FlinkKafkaProducer 实现 TwoPhaseCommitSinkFunction 时，这些方法的调用顺序是：
 * invoke()：此方法首先被调用，它根据收到的标识符将行添加到事务中。
 * beginTransaction()：在invoke()方法之后调用，它返回一个事务标识符。
 * preCommit()：在beginTransaction()方法之后调用，它对当前事务数据进行所有最终修改。
 * commit()：在preCommit()方法之后调用，它处理已完成的预提交数据事务。
 * abort()：此方法在commit()方法之后调用，它用于取消事务
 */
public class TwoPhaseFileSink extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>, String, Void> {

    // 使用 Flink 的状态管理器管理 buffer
    private ListState<Integer> checkpointedState;
    // 缓存
    private List<String> buffer;
    private String preCommitPath;
    private String commitedPath;


    public TwoPhaseFileSink(String preCommitPath, String commitedPath) {
        super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
        this.preCommitPath = preCommitPath;
        this.commitedPath = commitedPath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initBuffer();
    }

    //作用：对于流中的每个元素，都会调用此方法，将元素添加到当前事务中。
    //step2 调用时机：在事务中，每来一个新的数据元素时。
    @Override
    protected void invoke(String transaction, Tuple2<String, Integer> value, Context context) throws Exception {
        // 缓存即将写入文件的数据
        buffer.add(value.f0);
//        checkpointedState.add(value.f1); // Add to the checkpointed state as well
    }

    //作用：该方法在处理每个检查点时首先被调用，用于开始一个新的事务。
    //调用时机：当需要开始新的事务以准备写入数据时。
    @Override
    protected String beginTransaction() throws Exception {
        //step1:开启一个新事物
        String time = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        time = time.substring(0, 19).replace(":","_");
        int subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        String fileName = time + "-" + subTaskIdx + ".log";
        initBuffer();
        if (buffer == null) {
            buffer = new ArrayList<>();
        }
        buffer.clear(); // Clear local in-memory buffer
        // Clear previous state
//        checkpointedState.clear();
        return fileName;

    }

    //作用：预提交当前正在进行的事务，在实际完成提交之前可以执行例如清理、预写等操作。
    //step3 调用时机：在检查点发生时，且在任何最终的commit()调用之前。
    @Override
    protected void preCommit(String transaction) throws Exception {

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(preCommitPath + transaction))) {
            for (String line : buffer) {
                bufferedWriter.write(line);
                bufferedWriter.newLine();//换行
            }
        }
        // 清空 buffer 是一个选择，取决于是否希望在失败时重新发送数据
//         buffer.clear();
    }

    //作用：确认并完成事务的提交，持久化事务中的所有更改。
    //step4 调用时机：在成功的检查点后，preCommit()调用之后。
    @Override
    protected void commit(String transaction) {

        Path preCommitFilePath = Paths.get(preCommitPath + transaction);
        if (Files.exists(preCommitFilePath)) {
            Path commitedFilePath = Paths.get(commitedPath + "target.log");
            try {
                Files.write(
                        commitedFilePath,
                        Files.readAllBytes(preCommitFilePath),
                        StandardOpenOption.CREATE, // 如果文件不存在则创建
                        StandardOpenOption.APPEND   // 追加模式写入
                );
                // 删除preCommitFilePath文件
                Files.delete(preCommitFilePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //作用：如果事务需要回滚，或者在恢复时遇到未完成的事务，则调用此方法来撤销任何已做的更改。
    //调用时机：当事务需要被中止时。
    @Override
    protected void abort(String transaction) {
        Path preCommitFilePath = Paths.get(preCommitPath + transaction);
        if (Files.exists(preCommitFilePath)) {
            try {
                Files.delete(preCommitFilePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void initBuffer() {
        if (buffer == null) {
            buffer = new ArrayList<>();
        }
//        if (checkpointedState == null) {
//            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>(
//                    "buffered-elements",
//                    TypeInformation.of(new TypeHint<Integer>() {
//                    })
//            );
//            checkpointedState = getRuntimeContext().getListState(descriptor);
//        }
    }
}
