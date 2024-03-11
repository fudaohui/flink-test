package org.fdh.day06;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.fdh.bean.stock.StockPrice;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

public class SinkToKafkaDemo {

    public void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple2<String, Double>> stream = env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
                .addSource(new StockSourceCheckPoint("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                ).keyBy(StockPrice::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String s
                            , ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow>.Context context
                            , Iterable<StockPrice> elements
                            , Collector<Tuple2<String, Double>> out) throws Exception {

                        int totalVolume = 0;
                        double totalPrice = 0;
                        for (StockPrice element : elements) {
                            totalVolume += element.getVolume();
                            totalPrice += element.getPrice() * element.getVolume();
                        }
                        out.collect(Tuple2.of(s, totalPrice / totalVolume));
                    }
                });
        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");
        String outputTopic = "stocks";
        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer(outputTopic, new KafkaStockSerializationSchema(outputTopic)
                , properties, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        stream.addSink(flinkKafkaProducer);
        stream.print();
    }

    class  KafkaStockSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Double>>{

        private String topic;

        public KafkaStockSerializationSchema(String topic) {
            super();
            this.topic = topic;
        }
        @Override
        public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Double> element, @Nullable Long ts) {
            return new ProducerRecord<byte[], byte[]>(topic, (element.f0 + ": " + element.f1).getBytes(StandardCharsets.UTF_8));
        }
    }
}
