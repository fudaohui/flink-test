package org.fdh.day02;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.fdh.bean.stock.StockPrice;
import org.fdh.bean.stock.StockPriceSource;

/**
 * 过滤成交量大于阈值200的股票数据
 */
public class StockPriceFilter {

    public static void main(String[] args) throws Exception {
        int volumeThreshold = 200;
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<StockPrice> stock = executionEnvironment.addSource(new StockPriceSource("stock/stock-tick-20200108.csv"), "stock")
                .filter(stockPrice -> stockPrice.volume > volumeThreshold);
        stock.print();
        executionEnvironment.execute("stock-price-large-volume");
    }
}
