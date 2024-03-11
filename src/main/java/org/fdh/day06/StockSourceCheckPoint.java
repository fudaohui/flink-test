package org.fdh.day06;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.fdh.bean.stock.StockPrice;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StockSourceCheckPoint extends RichSourceFunction<StockPrice> implements CheckpointedFunction {

    private Integer offset;
    private Integer lastOffset;
    private String path;
    private boolean isRunning = true;
    private InputStream inputStream;

    private ListState<Integer> offsetState;

    public StockSourceCheckPoint(String path) {
        this.path = path;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        offsetState.add(offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>("offset", Types.INT);
        offsetState = context.getOperatorStateStore().getListState(stateDescriptor);
        Iterable<Integer> iterable = offsetState.get();
        if (iterable == null || !iterable.iterator().hasNext()) {
            lastOffset = 0;
        } else {
            lastOffset = iterable.iterator().next();
        }

    }

    @Override
    public void run(SourceContext<StockPrice> ctx) throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
        String line;
        boolean isFirstLine = true;
        long lastTs = 0;
        inputStream = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        while (isRunning && !StringUtils.isEmpty(line = bufferedReader.readLine())) {
            if (offset < lastOffset) {
                offset++;
                continue;
            }
            String[] split = line.split(",");
            LocalDateTime dateTime = LocalDateTime.parse(split[1] + " " + split[2], formatter);
            long eventTs = Timestamp.valueOf(dateTime).getTime();
            StockPrice stockPrice = new StockPrice();
            stockPrice.setSymbol(split[0]);
            stockPrice.setTs(eventTs);
            stockPrice.setPrice(Double.parseDouble(split[3]));
            stockPrice.setVolume(Integer.parseInt(split[4]));
            if (isFirstLine) {
                lastTs = eventTs;
                isFirstLine = false;
            }
            if ((eventTs - lastTs) > 0) {
                Thread.sleep(eventTs - lastTs);
            }
            //执行checkpoint的时候暂时像下游发送数据
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(stockPrice);
                offset++;
            }
            lastTs = eventTs;
        }
    }

    @Override
    public void cancel() {
        try {
            inputStream.close();
            isRunning = false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
