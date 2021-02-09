package cn.yintech.flink.dataStream.asyncRequestTest;
import com.alibaba.fastjson.JSON;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
public class AsyncMysqlRequest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092");
//        properties.setProperty("auto.offset.reset", "earliest");
//        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "AsyncMysqlRequest");
        String topic = "test_topic";

        FlinkKafkaConsumer011<ObjectNode> source = new FlinkKafkaConsumer011<>(topic, new MyJSONKeyValueDeserializationSchema(true), properties);
        final DataStream<AsyncUser> input = env.addSource(source).map(value -> {
            String id = "0";
            String username = "null";
            String password = "null";
            try {
                id = value.get("value").get("id").asText();
                username = value.get("value").get("username").asText();
                password = value.get("value").get("password").asText();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return new AsyncUser(id, username, password,"null");
        });
        // 异步IO 获取mysql数据, timeout 时间 1s，容量 10（超过10个请求，会反压上游节点）
        DataStream<AsyncUser> async = AsyncDataStream.unorderedWait(input,new AsyncFunctionForMysqlJava(),5000,TimeUnit.MICROSECONDS,10);
        async.map(user -> JSON.toJSON(user).toString()).print();

        env.execute("asyncForMysql");

    }

}
