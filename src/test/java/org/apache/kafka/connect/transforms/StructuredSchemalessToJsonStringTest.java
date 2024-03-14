package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StructuredSchemalessToJsonStringTest {
    private final StructuredSchemalessToJsonString<SinkRecord> xform = new StructuredSchemalessToJsonString.Key<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void unstructuredSchemaless() {
        xform.configure(Collections.emptyMap());

        final SinkRecord record = new SinkRecord("test", 0, null, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertEquals(42, transformedRecord.key());
    }

    @Test
    public void structuredSchemaless() {
        xform.configure(Collections.emptyMap());

        final SinkRecord record = new SinkRecord("test", 0, null, Map.of("id", 42), null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertEquals("{\"id\":42}", transformedRecord.key());
    }

    @Test
    public void structuredSchema() {
        xform.configure(Collections.emptyMap());

        Schema originalSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();

        Struct input = new Struct(originalSchema)
                .put("id", 42);

        final SinkRecord record = new SinkRecord("test", 0, originalSchema, input, null, null, 0);
        assertEquals(record, xform.apply(record));
    }
}