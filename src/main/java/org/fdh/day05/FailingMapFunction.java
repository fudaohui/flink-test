package org.fdh.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * 注意调用该类，并行度设置为1
 */
public class FailingMapFunction implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private int count;

    private int failInterval;

    public FailingMapFunction(int failInterval) {
        this.failInterval = failInterval;
    }

    @Override
    public Tuple2<String, Integer> map(Tuple2<String, Integer> in) throws Exception {
        this.count += 1;
        if (this.count > failInterval) {
            System.out.println("job fail! show how flink checkpoint and recovery");
            count = 0;
            throw new RuntimeException("job fail! show how flink checkpoint and recovery");
        }
        return in;
    }
}
