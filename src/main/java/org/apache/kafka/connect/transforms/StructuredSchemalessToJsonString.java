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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class StructuredSchemalessToJsonString<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Transforms schemaless structured data to a simple json String."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                    + "or value (<code>" + Value.class.getName() + "</code>)." +
                    "<p/>This SMT let untouched data with a schema and unstructured data.";
    public static final ConfigDef CONFIG_DEF = new ConfigDef();
    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredSchemalessToJsonString.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> props) {

    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        Object value = operatingValue(record);

        if (schema == null) {
            try {
                if (value instanceof Map) {
                    value = OBJECT_MAPPER.writeValueAsString(value);
                    return newRecord(record, value);
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

        }

        return record;
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends StructuredSchemalessToJsonString<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), null, updatedValue, record.valueSchema(), record.value(), record.timestamp(), record.headers());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends StructuredSchemalessToJsonString<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, updatedValue, record.timestamp(), record.headers());
        }
    }

}