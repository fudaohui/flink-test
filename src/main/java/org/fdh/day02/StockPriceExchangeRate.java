package org.fdh.day02;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.fdh.bean.stock.StockPrice;
import org.fdh.bean.stock.StockPriceSource;

/**
 * 将股票价格转换为美元
 */
public class StockPriceExchangeRate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> outputStreamOperator = executionEnvironment.addSource(new StockPriceSource("stock/stock-tick-20200108.csv"), "stock")
                .map(stockPrice -> {
                    StockPrice stockPriceNew = new StockPrice();
                    stockPriceNew.setPrice(stockPrice.getPrice() * 7);
                    stockPriceNew.setTs(stockPrice.getTs());
                    stockPriceNew.setVolume(stockPrice.getVolume());
                    stockPriceNew.setSymbol(stockPrice.getSymbol());
                    return stockPriceNew;
                });
        outputStreamOperator.print();
        executionEnvironment.execute("stock-price-exchange-rate");
    }
}
