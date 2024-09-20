package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public abstract class DropField<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String FIELDS_CONFIG = "fields";
    public static final String FIELDS_DOC = "Field names on the record to drop. Accept nested fields notation as defined by KIP-821";

    private List<String> fields = new ArrayList<>();

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.HIGH, FIELDS_DOC);

    private static final String PURPOSE = "Drop field";

    @Override
    public void configure(Map<String, ?> properties) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, properties);
        fields = config.getList(FIELDS_CONFIG);
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }

    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = deepValueCopySchemaless(value, "");

        return newRecord(record, null, updatedValue);
    }

    boolean filter(String path) {
        return !fields.contains(path);
    }

    private Map<String, Object> deepValueCopySchemaless(Map<String, Object> original, String path) {
        final Map<String, Object> updatedValue = new HashMap<>(original.size());

        for (Map.Entry<String, Object> e : original.entrySet()) {
            final String fieldName = e.getKey();
            String fieldPath = path + fieldName;
            Object fieldValue = e.getValue();
            if (e.getValue() instanceof Map) {
                fieldValue = deepValueCopySchemaless((Map<String, Object>) fieldValue, fieldPath + ".");
            }
            if (filter(fieldPath)) {
                updatedValue.put(fieldName, fieldValue);
            }
        }
        return updatedValue;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = deepSchemaCopy(value.schema(), "");

        final Struct updatedValue = deepValueCopyWithSchema(value, updatedSchema);

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Struct deepValueCopyWithSchema(Struct original, Schema schema) {
        final Struct updatedValue = new Struct(schema);
        for (Field field : schema.fields()) {
            Object fieldValue = original.get(field.name());
            if (field.schema().type() == Schema.Type.STRUCT) {
                fieldValue = deepValueCopyWithSchema(((Struct) fieldValue), field.schema());
            }
            updatedValue.put(field.name(), fieldValue);
        }
        return updatedValue;
    }

    private Schema deepSchemaCopy(Schema schema, String path) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            String fieldPath = path + field.name();
            Schema fieldSchema = field.schema();
            if (fieldSchema.type() == Schema.Type.STRUCT) {
                fieldSchema = deepSchemaCopy(field.schema(), fieldPath + ".");
            }
            if (filter(fieldPath)) {
                builder.field(field.name(), fieldSchema);
            }
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);


    public static class Key<R extends ConnectRecord<R>> extends DropField<R> {

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
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends DropField<R> {

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
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}


