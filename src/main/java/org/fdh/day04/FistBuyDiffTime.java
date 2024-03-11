package org.fdh.day04;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.fdh.bean.taobao.UserBehavior;
import org.fdh.bean.taobao.UserBehaviorSource;

import java.time.Duration;

/**
 * 使用键控状态记录用户购买行为：第一次发生购买行为的时间(从点击到购买的时间)
 */
public class FistBuyDiffTime {



    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp());
        DataStream<Tuple2<Long, Integer>> stream = env.addSource(new UserBehaviorSource("taobao/UserBehavior-test.csv"), "user-behavior-source")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(UserBehavior::getUserId)
                .process(new TimeDiffFunction());

        stream.print();
        env.execute("time-diffa");
    }


    static class TimeDiffFunction extends KeyedProcessFunction<Long, UserBehavior, Tuple2<Long, Integer>> {


        private ValueState<Long> firstPvActionState;
        private ValueState<Long> firstBuyActionState;

        @Override
        public void processElement(UserBehavior value
                , KeyedProcessFunction<Long, UserBehavior, Tuple2<Long, Integer>>.Context ctx
                , Collector<Tuple2<Long, Integer>> out) throws Exception {

            if (firstPvActionState.value() == null && value.getBehavior().equals("pv")) {
                firstPvActionState.update(value.getTimestamp());
            }

            if (firstBuyActionState.value() == null && value.getBehavior().equals("buy")) {
                firstBuyActionState.update(value.getTimestamp());
            }

            if (firstPvActionState.value() != null && firstBuyActionState.value() != null) {
                out.collect(Tuple2.of(value.getUserId(), (int) (firstBuyActionState.value() - firstPvActionState.value())));
            }

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> firstActionStateDescriptor = new ValueStateDescriptor<>("firstPvActionState", Long.class);
            firstPvActionState = getRuntimeContext().getState(firstActionStateDescriptor);
            ValueStateDescriptor<Long> buyActionStateDescriptor = new ValueStateDescriptor<>("firstBuyActionState", Long.class);
            firstBuyActionState = getRuntimeContext().getState(buyActionStateDescriptor);
        }
    }

}
