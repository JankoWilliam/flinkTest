package cn.yintech.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.DeserializerCache;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.Timestamp;

public class FlinkTableKafkaTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);


        tableEnv.connect(
                new Kafka()
                        .version("0.10")
                        .topic("sc_md")
//                        .property("zookeeper.connect", "bigdata002:2181,bigdata003:2181,bigdata004:2181")
//                        .property("bootstrap.servers", "bigdata002:9092,bigdata003:9092,bigdata004:9092")
                        .property("zookeeper.connect", "bigdata002.sj.com:2181,bigdata003.sj.com:2181,bigdata004.sj.com:2181")
                        .property("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092")
                        .property("group.id", "LiveVisitCountFlink02")
                        .startFromGroupOffsets()

        ).withFormat(
                new Json()
                        .failOnMissingField(false)
//                        .jsonSchema("{" +
//                                "   event:'string'," +
//                                "   properties:{" +
//                                "       v1_message_title:'string'" +
//                                "   }" +
//                                "}")
                        .deriveSchema()
        ).withSchema(
                new Schema()
                        .field("event", Types.STRING())
                        .field("time", Types.LONG())
                        .field("properties",Types.ROW(
                                new String[]{
                                        "userID",
                                        "v1_message_title",
                                        "v1_message_id"},
                                new TypeInformation[]{
                                    Types.STRING(),
                                    Types.STRING(),
                                    Types.STRING()
                        }))
//                        .field("proctime", Types.SQL_TIMESTAMP()).proctime()

        )
                .inAppendMode()
                .registerTableSource("eventlog");

        tableEnv.registerFunction("utc2local", new UTC2Local());

        Table liveVisit = tableEnv.sqlQuery("select event,userID,v1_message_title,v1_message_id,`time` from eventlog where event = 'LiveVisit'");

        SingleOutputStreamOperator<Row> liveVisitDataStream = tableEnv.toAppendStream(liveVisit, Row.class)
                .assignTimestampsAndWatermarks(new TimeStampExtractor())
                .returns(new RowTypeInfo(Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.LONG()))
                ;

        Table liveVisitTable = tableEnv.fromDataStream(liveVisitDataStream, "event,userID,v1_message_title,v1_message_id,time.rowtime");
        tableEnv.registerTable("live_visit",liveVisitTable);

        String sql = "SELECT v1_message_id,v1_message_title," +
                "utc2local(HOP_START(`time`, INTERVAL '5' SECOND,INTERVAL '1' DAY)) as hStart," +
                "count(DISTINCT userID) AS countNum " +
                "from live_visit where v1_message_id is not null " +
                "GROUP BY HOP(`time`,INTERVAL '5' SECOND,INTERVAL '1' DAY),v1_message_id,v1_message_title";

        Table table = tableEnv.sqlQuery(sql);
        DataStream<Row> dataStream = tableEnv.toAppendStream(table, Row.class);

//        dataStream.writeUsingOutputFormat(new HBaseRichOutputFormat());


        liveVisit.printSchema();
        dataStream.print();


        tableEnv.execute("LiveVisitCountFlink2");

    }

    public static class UTC2Local extends ScalarFunction implements Serializable {
        public Timestamp eval(Timestamp s) {
            long timestamp = s.getTime() + 28800000;
            return new Timestamp(timestamp);
        }

        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.SQL_TIMESTAMP();
        }

    }

    static class TimeStampExtractor implements Serializable, AssignerWithPeriodicWatermarks<Row> {
        long maxOutOfOrderness = 5000L ;// 3.5 seconds
        long currentMaxTimestamp ;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Row row, long l) {
            long timestamp = (long) row.getField(4);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

}
