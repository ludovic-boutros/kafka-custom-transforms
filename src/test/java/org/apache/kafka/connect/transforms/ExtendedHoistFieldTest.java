/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ExtendedHoistFieldTest {
    private final ExtendedHoistField<SinkRecord> xform = new ExtendedHoistField.Key<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        xform.configure(Collections.singletonMap("field", "magic"));

        final SinkRecord record = new SinkRecord("test", 0, null, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertEquals(Collections.singletonMap("magic", 42), transformedRecord.key());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void schemalessKeepInRootFields() {
        xform.configure(Map.of("field", "magic", "keepInRootFieldNames", "keepInRoot,keepInRoot2"));

        final SinkRecord record = new SinkRecord("test", 0, null,
                Map.of(
                        "shouldBeMoved", 42,
                        "keepInRoot", 111,
                        "keepInRoot2", 112),
                null, null, 0);

        final SinkRecord transformedRecord = xform.apply(record);

        Map<String, Object> key = (Map<String, Object>) transformedRecord.key();
        assertNull(transformedRecord.keySchema());
        assertEquals(111, key.get("keepInRoot"));
        assertEquals(112, key.get("keepInRoot2"));
        assertEquals(Map.of(
                "shouldBeMoved", 42), key.get("magic"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void schemalessNoChange() {
        xform.configure(Map.of("field", "magic", "keepInRootFieldNames", "keepInRoot,keepInRoot2"));

        final SinkRecord record = new SinkRecord("test", 0, null,
                Map.of(
                        "keepInRoot", 111,
                        "keepInRoot2", 112),
                null, null, 0);

        final SinkRecord transformedRecord = xform.apply(record);

        Map<String, Object> key = (Map<String, Object>) transformedRecord.key();
        assertNull(transformedRecord.keySchema());
        assertEquals(111, key.get("keepInRoot"));
        assertEquals(112, key.get("keepInRoot2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void schemalessKeepInRootEmptyFields() {
        xform.configure(Map.of("field", "magic", "keepInRootFieldNames", "keepInRoot,keepInRoot2"));

        final SinkRecord record = new SinkRecord("test", 0, null,
                Map.of(
                        "keepInRoot", 111,
                        "keepInRoot2", 112),
                null, null, 0);

        final SinkRecord transformedRecord = xform.apply(record);

        Map<String, Object> key = (Map<String, Object>) transformedRecord.key();
        assertNull(transformedRecord.keySchema());
        assertEquals(111, key.get("keepInRoot"));
        assertEquals(112, key.get("keepInRoot2"));
        assertNull(key.get("magic"));
    }

    @Test
    public void schemaKeepInRootFields() {

        Schema originalSchema = SchemaBuilder.struct()
                .field("shouldBeMoved", Schema.INT32_SCHEMA)
                .field("keepInRoot", Schema.INT32_SCHEMA)
                .field("keepInRoot2", Schema.INT32_SCHEMA)
                .build();

        Struct input = new Struct(originalSchema)
                .put("shouldBeMoved", 42)
                .put("keepInRoot", 111)
                .put("keepInRoot2", 112);

        xform.configure(Map.of("field", "magic", "keepInRootFieldNames", "keepInRoot,keepInRoot2"));

        final SinkRecord record = new SinkRecord("test", 0, originalSchema, input, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(111, ((Struct) transformedRecord.key()).get("keepInRoot"));
        assertEquals(112, ((Struct) transformedRecord.key()).get("keepInRoot2"));
        assertEquals(42, ((Struct) transformedRecord.key()).getStruct("magic").get("shouldBeMoved"));
    }

    @Test
    public void schemaNoChange() {

        Schema originalSchema = SchemaBuilder.struct()
                .field("keepInRoot", Schema.INT32_SCHEMA)
                .field("keepInRoot2", Schema.INT32_SCHEMA)
                .build();

        Struct input = new Struct(originalSchema)
                .put("keepInRoot", 111)
                .put("keepInRoot2", 112);

        xform.configure(Map.of("field", "magic", "keepInRootFieldNames", "keepInRoot,keepInRoot2"));

        final SinkRecord record = new SinkRecord("test", 0, originalSchema, input, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(111, ((Struct) transformedRecord.key()).get("keepInRoot"));
        assertEquals(112, ((Struct) transformedRecord.key()).get("keepInRoot2"));
    }

    @Test
    public void schemaKeepInRootEmptyInnerFields() {

        Schema originalSchema = SchemaBuilder.struct()
                .field("shouldBeMoved", Schema.OPTIONAL_INT32_SCHEMA)
                .field("keepInRoot", Schema.INT32_SCHEMA)
                .field("keepInRoot2", Schema.INT32_SCHEMA)
                .build();

        Struct input = new Struct(originalSchema)
                .put("keepInRoot", 111)
                .put("keepInRoot2", 112);

        xform.configure(Map.of("field", "magic", "keepInRootFieldNames", "keepInRoot,keepInRoot2"));

        final SinkRecord record = new SinkRecord("test", 0, originalSchema, input, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(111, ((Struct) transformedRecord.key()).get("keepInRoot"));
        assertEquals(112, ((Struct) transformedRecord.key()).get("keepInRoot2"));
        assertNull(((Struct) transformedRecord.key()).getStruct("magic"));
    }

    @Test
    public void withSchema() {
        xform.configure(Collections.singletonMap("field", "magic"));

        final SinkRecord record = new SinkRecord("test", 0, Schema.INT32_SCHEMA, 42, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.Type.STRUCT, transformedRecord.keySchema().type());
        assertEquals(record.keySchema(), transformedRecord.keySchema().field("magic").schema());
        assertEquals(42, ((Struct) transformedRecord.key()).get("magic"));
    }

    @Test
    public void testSchemalessMapIsMutable() {
        xform.configure(Collections.singletonMap("field", "magic"));

        final SinkRecord record = new SinkRecord("test", 0, null, 420, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.keySchema());
        @SuppressWarnings("unchecked")
        Map<String, Object> actualKey = (Map<String, Object>) transformedRecord.key();
        actualKey.put("k", "v");
        Map<String, Object> expectedKey = new HashMap<>();
        expectedKey.put("k", "v");
        expectedKey.put("magic", 420);
        assertEquals(expectedKey, actualKey);
    }
}