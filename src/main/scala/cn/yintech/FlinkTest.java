package cn.yintech;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> tuple2DataStream = env.fromElements(new Tuple2<>("a", 1),
                new Tuple2<>("a", 2),
                new Tuple2<>("b", 3),
                new Tuple2<>("b", 5));
        // KeyBy -> Reduce
        final KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = tuple2DataStream.keyBy(0);
        keyedStream.reduce((ReduceFunction<Tuple2<String, Integer>>) (v1, v2) ->
                new Tuple2<>(v1.f0, v1.f1 + v2.f1)
        ).print("111");
        // Aggregations [KeyedStream → DataStream]
        tuple2DataStream.keyBy(0).sum(1).print("222");

        env.execute("FlinkTest");

    }

}
