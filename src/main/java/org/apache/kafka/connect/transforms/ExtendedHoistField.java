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

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class ExtendedHoistField<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Wrap data using the specified field name in a Struct when schema present, or a Map in the case of schemaless data."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                    + "or value (<code>" + Value.class.getName() + "</code>).";
    private static final Logger log = LoggerFactory.getLogger(ExtendedHoistField.class);
    private static final String FIELD_CONFIG = "field";
    private static final String KEEP_IN_ROOT_FIELD_CONFIG = "keepInRootFieldNames";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Field name for the single field that will be created in the resulting Struct or Map.")
            .define(KEEP_IN_ROOT_FIELD_CONFIG, ConfigDef.Type.LIST, List.of(), ConfigDef.Importance.MEDIUM,
                    "Field names which should be kept at root level.");
    private Cache<Schema, Schema> schemaUpdateCache;

    private String fieldName;
    private Set<String> keepInRootFieldNames;

    @SuppressWarnings("unchecked")
    private Map<String, Object> getValueAsMap(Object value) {
        Map<String, Object> valueAsMap;
        if (value instanceof Map) {
            valueAsMap = (Map<String, Object>) value;
        } else {
            throw new IllegalArgumentException("Bad record type. Should be a Map instance.");
        }
        return valueAsMap;
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(FIELD_CONFIG);
        keepInRootFieldNames = new HashSet<>(config.getList(KEEP_IN_ROOT_FIELD_CONFIG));
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        Object value = operatingValue(record);

        if (schema == null) {
            Map<String, Object> updatedValue = new HashMap<>();
            if (keepInRootFieldNames.isEmpty()) {

                updatedValue.put(fieldName, value);
            } else {
                Map<String, Object> valueAsMap;
                valueAsMap = getValueAsMap(value);

                Map<String, Object> innerValue = new HashMap<>();

                valueAsMap.forEach((k, v) -> {
                    if (keepInRootFieldNames.contains(k)) {
                        updatedValue.put(k, v);
                    } else {
                        innerValue.put(k, v);
                    }
                });

                if (!innerValue.isEmpty()) {
                    updatedValue.put(fieldName, innerValue);
                }
            }

            return newRecord(record, null, updatedValue);
        } else {
            Schema updatedSchema = schemaUpdateCache.get(schema);
            final Struct updatedValue;
            if (value instanceof Struct) {
                final Struct valueStruct = (Struct) value;

                if (updatedSchema == null) {
                    SchemaBuilder rootSchema = SchemaBuilder.struct();
                    SchemaBuilder innerSchema = SchemaBuilder.struct();

                    boolean somethingInInnerValue = schema.fields().stream().map(f -> {
                        if (keepInRootFieldNames.contains(f.name())) {
                            rootSchema.field(f.name(), f.schema());
                            return false;
                        } else {
                            innerSchema.field(f.name(), f.schema());
                            return true;
                        }
                    }).reduce(false, Boolean::logicalOr);

                    if (somethingInInnerValue) {
                        rootSchema.field(fieldName, innerSchema);
                    }
                    updatedSchema = rootSchema.build();
                    schemaUpdateCache.put(schema, updatedSchema);
                }

                if (updatedSchema.field(fieldName) != null) {
                    updatedValue = new Struct(updatedSchema);

                    final Struct innerValue = new Struct(updatedSchema.field(fieldName).schema());

                    boolean somethingInInnerValue = schema.fields().stream().map(f -> {
                        if (valueStruct.get(f) == null) {
                            return false;
                        }

                        if (keepInRootFieldNames.contains(f.name())) {
                            updatedValue.put(f.name(), valueStruct.get(f));
                            return false;
                        } else {
                            innerValue.put(f.name(), valueStruct.get(f));
                            return true;
                        }
                    }).reduce(false, Boolean::logicalOr);

                    if (somethingInInnerValue) {
                        updatedValue.put(fieldName, innerValue);
                    }
                } else {
                    return record;
                }
            } else {
                if (updatedSchema == null) {
                    updatedSchema = SchemaBuilder.struct().field(fieldName, schema).build();
                    schemaUpdateCache.put(schema, updatedSchema);
                }

                updatedValue = new Struct(updatedSchema).put(fieldName, value);
            }
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ExtendedHoistField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp(), record.headers());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ExtendedHoistField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp(), record.headers());
        }
    }

}