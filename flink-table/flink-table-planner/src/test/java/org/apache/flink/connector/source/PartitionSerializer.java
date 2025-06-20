package org.apache.flink.connector.source;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.connector.source.partitioning.KeyGroupedPartitioning;
import org.apache.flink.table.connector.source.partitioning.Partitioning;
import org.apache.flink.table.expressions.TransformExpression;
import org.apache.flink.types.Row;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for serializing and deserializing Partitioning instances to/from JSON.
 */
public class PartitionSerializer {

    private static final ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();

    /**
     * Serializes a Partitioning instance to JSON string.
     * Currently only supports KeyGroupedPartitioning.
     *
     * @param partitioning the Partitioning instance to serialize
     * @return JSON string representation
     * @throws IOException if serialization fails
     * @throws IllegalArgumentException if partitioning is not a KeyGroupedPartitioning
     */
    public static String serialize(Partitioning partitioning) throws IOException {
        if (!(partitioning instanceof KeyGroupedPartitioning)) {
            throw new IllegalArgumentException(
                "Only KeyGroupedPartitioning is supported. Got: " + partitioning.getClass().getSimpleName());
        }

        KeyGroupedPartitioning keyGroupedPartitioning = (KeyGroupedPartitioning) partitioning;
        ObjectNode rootNode = objectMapper.createObjectNode();
        // Serialize numPartitions
        rootNode.put("numPartitions", keyGroupedPartitioning.numPartitions());
        // Serialize keys (TransformExpression array)
        ArrayNode keysNode = objectMapper.createArrayNode();
        for (TransformExpression key : keyGroupedPartitioning.keys()) {
            ObjectNode keyNode = objectMapper.createObjectNode();
            keyNode.put("key", key.getKey());
            if (key.getFunctionName().isPresent()) {
                keyNode.put("functionName", key.getFunctionName().get());
            }
            if (key.getNumBucketsOpt().isPresent()) {
                keyNode.put("numBuckets", key.getNumBucketsOpt().get());
            }
            keysNode.add(keyNode);
        }
        rootNode.set("keys", keysNode);
        // Serialize partition values (Row array)
        ArrayNode partitionValuesNode = objectMapper.createArrayNode();
        for (Row row : keyGroupedPartitioning.getPartitionValues()) {
            ArrayNode rowNode = objectMapper.createArrayNode();
            for (int i = 0; i < row.getArity(); i++) {
                Object field = row.getField(i);
                if (field == null) {
                    rowNode.addNull();
                } else {
                    // Convert field to JSON node based on its type
                    JsonNode fieldNode = objectMapper.valueToTree(field);
                    rowNode.add(fieldNode);
                }
            }
            partitionValuesNode.add(rowNode);
        }
        rootNode.set("partitionValues", partitionValuesNode);

        return objectMapper.writeValueAsString(rootNode);
    }

    /**
     * Deserializes a JSON string to Partitioning instance.
     * Currently only supports KeyGroupedPartitioning.
     *
     * @param json the JSON string to deserialize
     * @return Partitioning instance
     * @throws IOException if deserialization fails
     */
    public static Partitioning deserialize(String json) throws IOException {
        JsonNode rootNode = objectMapper.readTree(json);

        // Deserialize numPartitions
        int numPartitions = rootNode.get("numPartitions").asInt();

        // Deserialize keys
        JsonNode keysNode = rootNode.get("keys");
        List<TransformExpression> keysList = new ArrayList<>();
        for (JsonNode keyNode : keysNode) {
            String key = keyNode.get("key").asText();
            String functionName = keyNode.has("functionName") ? keyNode.get("functionName").asText() : null;
            Integer numBuckets = keyNode.has("numBuckets") ? keyNode.get("numBuckets").asInt() : null;

            TransformExpression transformExpression = new TransformExpression(key, functionName, numBuckets);
            keysList.add(transformExpression);
        }
        TransformExpression[] keys = keysList.toArray(new TransformExpression[0]);

        // Deserialize partition values
        JsonNode partitionValuesNode = rootNode.get("partitionValues");
        List<Row> partitionValuesList = new ArrayList<>();
        for (JsonNode rowNode : partitionValuesNode) {
            List<Object> fields = new ArrayList<>();
            for (JsonNode fieldNode : rowNode) {
                if (fieldNode.isNull()) {
                    fields.add(null);
                } else if (fieldNode.isTextual()) {
                    fields.add(fieldNode.asText());
                } else if (fieldNode.isInt()) {
                    fields.add(fieldNode.asInt());
                } else if (fieldNode.isLong()) {
                    fields.add(fieldNode.asLong());
                } else if (fieldNode.isDouble()) {
                    fields.add(fieldNode.asDouble());
                } else if (fieldNode.isBoolean()) {
                    fields.add(fieldNode.asBoolean());
                } else {
                    // For other types, try to convert to string
                    fields.add(fieldNode.asText());
                }
            }
            Row row = Row.of(fields.toArray());
            partitionValuesList.add(row);
        }
        Row[] partitionValues = partitionValuesList.toArray(new Row[0]);

        return new KeyGroupedPartitioning(keys, partitionValues, numPartitions);
    }
}
