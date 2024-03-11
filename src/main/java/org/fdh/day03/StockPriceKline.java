package org.fdh.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.fdh.bean.stock.StockPrice;
import org.fdh.bean.stock.StockPriceSource;

import java.time.Duration;

/**
 * 实时计算5min窗口内股票的最高价最低价开盘价收盘价
 */
public class StockPriceKline {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //注意提交到集群的时候不要使用本地环境，否则报错
//        Configuration conf = new Configuration();
//        conf.setInteger(RestOptions.PORT, 8082);
//        final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(conf);
        //事件最多延迟 2 秒钟到达
        WatermarkStrategy<StockPrice> watermarkStrategy = WatermarkStrategy.<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.getTs());
        SingleOutputStreamOperator<Tuple5<String, Double, Double, Double, Double>> stream = executionEnvironment
                .addSource(new StockPriceSource("stock/stock-tick-20200108.csv"), "stock-file")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(StockPrice::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new ProcessWindowFunction<StockPrice, Tuple5<String, Double, Double, Double, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        ProcessWindowFunction<StockPrice, Tuple5<String, Double, Double, Double, Double>, String, TimeWindow>.Context context,
                                        Iterable<StockPrice> elements,
                                        Collector<Tuple5<String, Double, Double, Double, Double>> out) throws Exception {

                        StockPrice stockPrice = elements.iterator().next();
                        double openPrice = stockPrice.price;
                        double highPrice = stockPrice.price;
                        double lowPrice = stockPrice.price;
                        double closePrice = stockPrice.price;
                        for (StockPrice element : elements) {
                            if (highPrice < element.price) {
                                highPrice = element.price;
                            }
                            if (lowPrice > element.price) {
                                lowPrice = element.price;
                            }
                            closePrice = element.price;
                        }
                        out.collect(Tuple5.of(s,openPrice,highPrice,lowPrice,closePrice));
                    }
                });
        stream.print();
        executionEnvironment.execute("OHLC stock price");
    }

}
