package cn.yintech.flink.dataStream.asyncRequestTest;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PublicEvolving
public class MyJSONKeyValueDeserializationSchema implements KafkaDeserializationSchema<ObjectNode> {
    private static final long serialVersionUID = 1509391548173891955L;
    private final boolean includeMetadata;
    private ObjectMapper mapper;
    private final static Logger log = LoggerFactory.getLogger(MyJSONKeyValueDeserializationSchema.class);

    public MyJSONKeyValueDeserializationSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    public ObjectNode deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        if (this.mapper == null) {
            this.mapper = new ObjectMapper();
        }

        ObjectNode node = this.mapper.createObjectNode();
        try {

            if (record.key() != null) {
                node.set("key", (JsonNode)this.mapper.readValue((byte[])record.key(), JsonNode.class));
            }

            if (record.value() != null) {
                node.set("value", (JsonNode)this.mapper.readValue((byte[])record.value(), JsonNode.class));
            }

            if (this.includeMetadata) {
                node.putObject("metadata").put("offset", record.offset()).put("topic", record.topic()).put("partition", record.partition());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
//            log.error("JSONKeyValueDeserializationSchema 出错：" + record.toString() + "=====key为" + new String(record.key()) + "=====数据为" + new String(record.value()));
        }

        return node;
    }

    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

    public TypeInformation<ObjectNode> getProducedType() {
        return TypeExtractor.getForClass(ObjectNode.class);
    }
}