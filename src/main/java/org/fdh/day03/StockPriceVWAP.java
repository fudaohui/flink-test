package org.fdh.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * 以5分钟为一个时间单位，计算xVWAP,注意有些场景在窗口上进行reduce操作更合适
 */
public class StockPriceVWAP {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<StockPrice> watermarkStrategy = WatermarkStrategy
                .<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, tms) -> element.getTs());
        SingleOutputStreamOperator<Tuple2<String, Double>> stream = env.addSource(new StockPriceSource("stock/stock-tick-20200108.csv"), "file")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(StockPrice::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                // 对于简单的聚合操作，如求和、求最大值等，reduce 是一个方便的选择。但其要求输出类型和输入类型一致,使用自定义的窗口处理函数笔记方便
//                .reduce((current,newData)->{
//
//                    current.price* current.getVolume()+newData.price* newData.getVolume()
//                })
                .process(new ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String s
                            , ProcessWindowFunction<StockPrice, Tuple2<String, Double>
                            , String, TimeWindow>.Context context, Iterable<StockPrice> elements
                            , Collector<Tuple2<String, Double>> out) throws Exception {

                        double priceVolumeSum = 0;
                        double volumeSum = 0;
                        for (StockPrice element : elements) {
                            volumeSum += element.getVolume();
                            priceVolumeSum = element.getPrice() + element.getVolume();
                        }
                        out.collect(Tuple2.of(s, priceVolumeSum / volumeSum));
                    }
                });

        stream.print();
        env.execute("VWAP-job");
    }
}
