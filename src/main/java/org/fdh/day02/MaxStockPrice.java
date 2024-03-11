package org.fdh.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.fdh.bean.stock.StockPrice;
import org.fdh.bean.stock.StockPriceSource;

/**
 * 实时计算某只股票的价格的最大值
 * 注意：我没有指定并行度，在idea本机执行的时候，无论如何设置总的并行度，
 * source都是一个，可能和数据源分片只有一个有关。文件只有一个，仅一个来读取防止并发读取
 */
public class MaxStockPrice {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8082);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        DataStream<StockPrice> maxPriceStream = env.addSource(new StockPriceSource("stock/stock-tick-20200108.csv"), "stock")
                .keyBy(data -> data.symbol)
                .max("price");
        maxPriceStream.print();
        env.execute("max-stock-price");
    }
}
