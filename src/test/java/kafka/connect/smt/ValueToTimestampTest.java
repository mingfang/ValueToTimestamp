package kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class ValueToTimestampTest {
    private final ValueToTimestamp<SinkRecord> xform = new ValueToTimestamp<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        xform.configure(Collections.singletonMap("field", "a"));
        Long timestamp = System.currentTimeMillis();

        final HashMap<String, Object> value = new HashMap<>();
        value.put("a", timestamp);

        final SinkRecord record = new SinkRecord("", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(timestamp, transformedRecord.timestamp());
    }

    @Test
    public void withSchema() {
        xform.configure(Collections.singletonMap("field", "a"));
        Long timestamp = System.currentTimeMillis();

        final Schema valueSchema = SchemaBuilder.struct()
                .field("a", Schema.INT64_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("a", timestamp);


        final SinkRecord record = new SinkRecord("", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(timestamp, transformedRecord.timestamp());
    }

    @Test
    public void nonExistingField() {
        xform.configure(Collections.singletonMap("field", "foo"));

        final Schema valueSchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("a", 1);

        final SinkRecord record = new SinkRecord("", 0, null, null, valueSchema, value, 0);

        DataException actual = org.junit.Assert.assertThrows(DataException.class, () -> xform.apply(record));
        assertEquals("foo is not a valid field name", actual.getMessage());
    }
}