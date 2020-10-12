package kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ValueToTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Replace the record timestamp with a new timestamp from a field in the record value.";

    public static final String FIELD_CONFIG = "field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "Field name on the record value to extract as the record timestamp.");

    private static final String PURPOSE = "copying field from value to timestamp";

    private String field;


    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        field = config.getString(FIELD_CONFIG);
    }

    @Override
    public R apply(R record) {
        Object timestamp;
        if (record.valueSchema() == null) {
            final Map<String, Object> value = requireMap(record.value(), PURPOSE);
            timestamp = value.get(field);
        } else {
            final Struct value = requireStruct(record.value(), PURPOSE);
            timestamp = value.get(field);
        }
        if(timestamp instanceof java.sql.Timestamp) {
            timestamp = ((java.sql.Timestamp)timestamp).getTime();
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), (Long) timestamp);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

}