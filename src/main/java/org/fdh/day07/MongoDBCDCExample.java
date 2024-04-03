package org.fdh.day07;

import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 这里面使用flink代码方式读取mongodb数据，后续可以和flink datastream api进行整合，这里只是打印到控制台输出
 * 注意：该代码在单实例上报错，仅支持副本集和分片集
 */
public class MongoDBCDCExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFunction<String> mongoSource = MongoDBSource.<String>builder()
                .hosts("10.5.7.77:27017")
                .username("admin")
                .password("J8sGvWLK8Wxl")
                .databaseList("test")
                .collectionList("test1")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        env.addSource(mongoSource)
                .print();

        env.execute("MongoDB CDC Example");
    }
}