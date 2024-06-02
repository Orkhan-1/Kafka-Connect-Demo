package com.demo.transformation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.LongNode;
import com.github.jcustenborder.kafka.connect.utils.data.Parser;
import com.demo.constant.TransformationConstants;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.avro.Schema;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonToRecordConverter<R extends ConnectRecord<R>> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToRecordConverter.class);

    public R convert(Schema keySchemaAvro, Schema valueSchemaAvro, R record) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node;
        try {
            node = mapper.readTree((String) record.value());
        } catch (IOException e) {
            throw new DataException("Value cannot be converted ", e);
        }

        Map<?, ?> configs = new HashMap<>();
        AvroData avroData = new AvroData(new AvroDataConfig(configs));

        org.apache.kafka.connect.data.Schema keySchema = avroData.toConnectSchema(keySchemaAvro);
        org.apache.kafka.connect.data.Schema valueSchema = avroData.toConnectSchema(valueSchemaAvro);

        LOG.debug("key: {} and value: {} has been converted to connect schema", keySchema.name(), valueSchema.name());

        Struct keyStruct = new Struct(keySchema);
        Struct valueStruct = new Struct(valueSchema);

        for (Field field : valueSchema.fields()) {
            JsonNode fieldNode = node.get(field.name());
            Object fieldValue;
            try {
                Parser parser = new Parser();

                if (field.schema().type().name().equalsIgnoreCase(TransformationConstants.INT_64)) {
                    fieldNode = changeJsonNodeType(field, fieldNode);
                }

                fieldValue = parser.parseJsonNode(field.schema(), fieldNode);
                valueStruct.put(field, fieldValue);

                Field keyField = keySchema.field(field.name());
                if (keyField != null) {
                    keyStruct.put(keyField, fieldValue);
                }
            } catch (Exception ex) {
                throw new DataException("Json cannot be parsed ", ex);
            }
        }

        SchemaAndValue key = new SchemaAndValue(keyStruct.schema(), keyStruct);
        SchemaAndValue value = new SchemaAndValue(valueStruct.schema(), valueStruct);
        return record.newRecord(record.topic(), record.kafkaPartition(),
                key.schema(), key.value(), value.schema(), value.value(), System.currentTimeMillis());
    }

    private JsonNode changeJsonNodeType(Field field, JsonNode fieldNode) {
        JsonNode changedJsonNode;
        if (field.name().equalsIgnoreCase(TransformationConstants.TIME_STAMP)) {
            changedJsonNode = new LongNode(System.currentTimeMillis());
        } else {
            changedJsonNode = new LongNode(fieldNode.longValue());
        }
        LOG.debug("type of node has been changed to {}", changedJsonNode.getNodeType());
        return changedJsonNode;
    }

}
