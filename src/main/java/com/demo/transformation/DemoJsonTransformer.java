package com.demo.transformation;

import com.demo.records.TestMessage;
import com.demo.records.TestMessageKey;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class DemoJsonTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(DemoJsonTransformer.class);

    private static final String KEY_CONFIG = "schema.name";
    private static final String CLASS_DOC = "Schema name";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(KEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, CLASS_DOC);

    @Override
    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    }

    @Override
    public R apply(R record) {
        LOG.debug("Input value {}", record.value());
        org.apache.avro.Schema keySchemaAvro = TestMessageKey.getClassSchema();
        org.apache.avro.Schema valueSchemaAvro = TestMessage.getClassSchema();
        JsonToRecordConverter<R> converter = new JsonToRecordConverter<>();
        return converter.convert(keySchemaAvro, valueSchemaAvro, record);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }


    @Override
    public void close() {
    }
}
