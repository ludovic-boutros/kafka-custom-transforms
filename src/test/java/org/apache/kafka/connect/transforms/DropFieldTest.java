package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DropFieldTest {

    private final DropField<SourceRecord> smt = new DropField.Value<>();

    @AfterEach
    public void close() {
        smt.close();
    }

    // Inspired by https://cwiki.apache.org/confluence/display/KAFKA/KIP-821%3A+Connect+Transforms+support+for+nested+structures#KIP821:ConnectTransformssupportfornestedstructures-ReplaceField
    @Test
    public void shouldNestedFieldDropFieldInStruct() {

        final Schema childSchema = SchemaBuilder.struct()
                .field("k2", SchemaBuilder.string());

        final Schema parentSchema = SchemaBuilder.struct()
                .field("child", childSchema);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("k1", SchemaBuilder.int32())
                .field("parent", parentSchema)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("k1", 123);
        value.put("parent", new Struct(parentSchema)
                .put("child", new Struct(childSchema)
                        .put("k2", "123")
                )
        );

        final Map<String, String> props = new HashMap<>();
        props.put("fields", "parent.child.k2");

        smt.configure(props);

        SourceRecord record = new SourceRecord(null, null, "test", 0, valueSchema, value);
        SourceRecord transformedRecord = smt.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(2, updatedValue.schema().fields().size());

        final Struct parentStruct = updatedValue.getStruct("parent");
        assertEquals(1, parentStruct.schema().fields().size());

        final Struct childStruct = parentStruct.getStruct("child");
        assertEquals(0, childStruct.schema().fields().size());
        assertNull(childStruct.schema().field("k2"));

    }

    @Test
    public void shouldNestedFieldDropStructInStruct() {

        final Schema childSchema = SchemaBuilder.struct()
                .field("k2", SchemaBuilder.string());

        final Schema parentSchema = SchemaBuilder.struct()
                .field("child", childSchema);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("k1", SchemaBuilder.int32())
                .field("parent", parentSchema)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("k1", 123);
        value.put("parent", new Struct(parentSchema)
                .put("child", new Struct(childSchema)
                        .put("k2", "123")
                )
        );

        final Map<String, String> props = new HashMap<>();
        props.put("fields", "parent.child");

        smt.configure(props);

        SourceRecord record = new SourceRecord(null, null, "test", 0, valueSchema, value);
        SourceRecord transformedRecord = smt.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(2, updatedValue.schema().fields().size());

        final Struct parentStruct = updatedValue.getStruct("parent");
        assertTrue(parentStruct.schema().fields().isEmpty());
        assertNull(parentStruct.schema().field("child"));
    }

    @Test
    public void shouldNestedFieldDropFieldInMap() {
        final Map<String, Object> value = Map.of(
                "k1", 123,
                "parent", Map.of(
                        "child", Map.of(
                                "k2", "123"
                        )
                )
        );

        final Map<String, String> props = new HashMap<>();
        props.put("fields", "parent.child.k2");

        smt.configure(props);

        SourceRecord record = new SourceRecord(null, null, "test", 0, null, value);
        SourceRecord transformedRecord = smt.apply(record);

        final Map<String, Object> updatedValue = (Map<String, Object>) transformedRecord.value();
        assertEquals(2, updatedValue.entrySet().size());

        final Map<String, Object> parentMap = (Map<String, Object>) updatedValue.get("parent");
        assertEquals(1, parentMap.entrySet().size());

        final Map<String, Object> childMap = (Map<String, Object>) parentMap.get("child");
        assertTrue(childMap.entrySet().isEmpty());
        assertNull(childMap.get("k2"));

    }

    @Test
    public void shouldNestedFieldDropStructInMap() {
        final Map<String, Object> value = Map.of(
                "k1", 123,
                "parent", Map.of(
                        "child", Map.of(
                                "k2", "123"
                        )
                )
        );

        final Map<String, String> props = new HashMap<>();
        props.put("fields", "parent.child");

        smt.configure(props);

        SourceRecord record = new SourceRecord(null, null, "test", 0, null, value);
        SourceRecord transformedRecord = smt.apply(record);

        final Map<String, Object> updatedValue = (Map<String, Object>) transformedRecord.value();
        assertEquals(2, updatedValue.entrySet().size());

        final Map<String, Object> parentMap = (Map<String, Object>) updatedValue.get("parent");
        assertTrue(parentMap.entrySet().isEmpty());
        assertNull(parentMap.get("child"));
    }


}
