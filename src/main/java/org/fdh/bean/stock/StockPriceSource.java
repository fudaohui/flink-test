package org.fdh.bean.stock;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 自定义source，没20s发送一帧股票数据
 */
public class StockPriceSource implements SourceFunction<StockPrice> {

    private boolean isRunning = true;
    private String path;
    private InputStream streamSource;

    public StockPriceSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<StockPrice> ctx) throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
        // 从项目的resources目录获取输入
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));
        String lineStr;
        while (isRunning && !StringUtils.isEmpty(lineStr = br.readLine())) {
            String[] split = lineStr.split(",");
            LocalDateTime dateTime = LocalDateTime.parse(split[1] + " " + split[2], formatter);
            long eventTs = Timestamp.valueOf(dateTime).getTime();
            StockPrice stockPrice = new StockPrice();
            stockPrice.setTs(eventTs);
            stockPrice.setSymbol(split[0]);
            stockPrice.setPrice(Double.parseDouble(split[3]));
            stockPrice.setVolume(Integer.parseInt(split[4]));
            ctx.collect(stockPrice);
            Thread.sleep(20);
        }
    }

    @Override
    public void cancel() {
        try {
            streamSource.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        isRunning = false;
    }
}
