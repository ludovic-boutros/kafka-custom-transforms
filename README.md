# Kafka Custom Transforms

## `DropField`
### Description
Drop one or many fields for each record. The SMT support nested fields;

Use the concrete transformation type designed for the record key (`com.github.danielpetisme.kafka.connect.smt.DropField$VKey`) or value (`com.github.danielpetisme.kafka.connect.smt.DropField$Value`).

### Example
These examples show how to configure and use `DropField`.

```
"transforms": "dropk2",
"transforms.dropk2.type": "org.apache.kafka.connect.transforms.DropField$Value",
"transforms.dropk2.fields": "parent.child.k2"
```

Before: `{"k1": 123, "parent": {"child": {"k2": "123"}}}`

After: `{"k1": 123, "parent": {"child": {}}}`

### Properties

|Name|Description|Type|Default|Valid Values|Importance|
|----|-----------|----|-------|------------|----------|
|`fields`| Field names on the record to drop. Accept nested fields notation|list||non-empty list|high| 
