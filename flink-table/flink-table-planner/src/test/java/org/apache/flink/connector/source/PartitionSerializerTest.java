package org.apache.flink.connector.source;

import org.apache.flink.table.connector.source.partitioning.KeyGroupedPartitioning;
import org.apache.flink.table.connector.source.partitioning.Partitioning;
import org.apache.flink.table.expressions.TransformExpression;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PartitionSerializer}.
 */
public class PartitionSerializerTest {

    @Test
    public void testSerializeAndDeserializeBasic() throws IOException {
        // Create test data
        TransformExpression[] keys = {
                new TransformExpression("dt", null, null),
                new TransformExpression("user_id", "bucket", 128)
        };

        Row[] partitionValues = {
                Row.of("2023-10-01", 0),
                Row.of("2023-10-01", 1),
                Row.of("2023-10-02", 0)
        };

        int numPartitions = 3;

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(
                keys,
                partitionValues,
                numPartitions);

        // Serialize
        String json = PartitionSerializer.serialize(original);
        assertNotNull(json);
        assertFalse(json.isEmpty());

        // Deserialize
        Partitioning deserialized = PartitionSerializer.deserialize(json);

        // Verify round-trip
        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized));
        assertEquals(original.numPartitions(), deserialized.numPartitions());
        assertEquals(original.keys().length, ((KeyGroupedPartitioning) deserialized).keys().length);
        assertEquals(
                original.getPartitionValues().length,
                ((KeyGroupedPartitioning) deserialized).getPartitionValues().length);
    }

    @Test
    public void testSerializeWithOnlyKeyNames() throws IOException {
        // Test with transform expressions that only have key names (no functions)
        TransformExpression[] keys = {
                new TransformExpression("year", null, null),
                new TransformExpression("month", null, null)
        };

        Row[] partitionValues = {
                Row.of(2023, 10),
                Row.of(2023, 11),
                Row.of(2024, 1)
        };

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(keys, partitionValues, 3);

        String json = PartitionSerializer.serialize(original);
        Partitioning deserialized = PartitionSerializer.deserialize(json);

        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized));
    }

    @Test
    public void testSerializeWithMixedDataTypes() throws IOException {
        // Test with various data types in partition values
        TransformExpression[] keys = {
                new TransformExpression("category", null, null),
                new TransformExpression("count", null, null),
                new TransformExpression("rate", null, null),
                new TransformExpression("active", null, null)};

        Row[] partitionValues = {
                Row.of("electronics", 100, 3.14, true),
                Row.of("books", 50, 2.75, false),
                Row.of("clothing", 200, 4.99, true)
        };

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(keys, partitionValues, 3);

        String json = PartitionSerializer.serialize(original);
        Partitioning deserialized = PartitionSerializer.deserialize(json);

        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized));

        // Verify specific values
        Row[] deserializedValues = ((KeyGroupedPartitioning) deserialized).getPartitionValues();
        assertEquals("electronics", deserializedValues[0].getField(0));
        assertEquals(100, deserializedValues[0].getField(1));
        assertEquals(3.14, deserializedValues[0].getField(2));
        assertEquals(true, deserializedValues[0].getField(3));
    }

    @Test
    public void testSerializeWithFunctionAndBuckets() throws IOException {
        // Test with transform expressions having function names and bucket counts
        TransformExpression[] keys = {
                new TransformExpression("user_id", "bucket", 256),
                new TransformExpression("timestamp", "hour", null),
                new TransformExpression("country", "hash", 64)
        };

        Row[] partitionValues = {
                Row.of(123, 14, 1),
                Row.of(45, 15, 2),
                Row.of(67, 16, 3)
        };

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(keys, partitionValues, 3);

        String json = PartitionSerializer.serialize(original);
        Partitioning deserialized = PartitionSerializer.deserialize(json);

        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized));

        // Verify transform expressions
        TransformExpression[] deserializedKeys = ((KeyGroupedPartitioning) deserialized).keys();
        assertEquals("user_id", deserializedKeys[0].getKey());
        assertEquals("bucket", deserializedKeys[0].getFunctionName().get());
        assertEquals(256, deserializedKeys[0].getNumBucketsOpt().get().intValue());

        assertEquals("timestamp", deserializedKeys[1].getKey());
        assertEquals("hour", deserializedKeys[1].getFunctionName().get());
        assertFalse(deserializedKeys[1].getNumBucketsOpt().isPresent());

        assertEquals("country", deserializedKeys[2].getKey());
        assertEquals("hash", deserializedKeys[2].getFunctionName().get());
        assertEquals(64, deserializedKeys[2].getNumBucketsOpt().get().intValue());
    }

    @Test
    public void testSerializeEmptyPartitionValues() throws IOException {
        // Test with empty partition values array
        TransformExpression[] keys = {
                new TransformExpression("dt", null, null)
        };

        Row[] partitionValues = {};

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(keys, partitionValues, 0);

        String json = PartitionSerializer.serialize(original);
        Partitioning deserialized = PartitionSerializer.deserialize(json);

        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized));
        assertEquals(0, ((KeyGroupedPartitioning) deserialized).getPartitionValues().length);
        assertEquals(0, deserialized.numPartitions());
    }

    @Test
    public void testSerializeWithLongValues() throws IOException {
        // Test with long values
        TransformExpression[] keys = {
                new TransformExpression("timestamp", null, null),
                new TransformExpression("id", null, null)
        };

        Row[] partitionValues = {
                Row.of(1698768000000L, 123456789012345L),
                Row.of(1698854400000L, 987654321098765L)
        };

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(keys, partitionValues, 2);

        String json = PartitionSerializer.serialize(original);
        Partitioning deserialized = PartitionSerializer.deserialize(json);

        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized));

        // Verify long values are preserved
        Row[] deserializedValues = ((KeyGroupedPartitioning) deserialized).getPartitionValues();
        assertEquals(1698768000000L, deserializedValues[0].getField(0));
        assertEquals(123456789012345L, deserializedValues[0].getField(1));
    }

    @Test
    public void testRoundTripConsistency() throws IOException {
        // Test multiple round trips to ensure consistency
        TransformExpression[] keys = {
                new TransformExpression("partition_key", "bucket", 100)
        };

        Row[] partitionValues = {
                Row.of("value1", 42),
                Row.of("value2", 84)
        };

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(keys, partitionValues, 2);

        // First round trip
        String json1 = PartitionSerializer.serialize(original);
        Partitioning deserialized1 = PartitionSerializer.deserialize(json1);

        // Second round trip
        String json2 = PartitionSerializer.serialize(deserialized1);
        Partitioning deserialized2 = PartitionSerializer.deserialize(json2);

        // Both should be compatible with original
        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized1));
        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized2));
        assertTrue(((KeyGroupedPartitioning) deserialized1).isCompatible((KeyGroupedPartitioning) deserialized2));

        // JSON strings should be identical
        assertEquals(json1, json2);
    }

    @Test
    public void testSerializeWithSingleKey() throws IOException {
        // Test with single key and single partition value
        TransformExpression[] keys = {
                new TransformExpression("single_key", null, null)
        };

        Row[] partitionValues = {
                Row.of("single_value")
        };

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(keys, partitionValues, 1);

        String json = PartitionSerializer.serialize(original);
        Partitioning deserialized = PartitionSerializer.deserialize(json);

        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized));
        assertEquals(1, ((KeyGroupedPartitioning) deserialized).keys().length);
        assertEquals(1, ((KeyGroupedPartitioning) deserialized).getPartitionValues().length);
        assertEquals(1, deserialized.numPartitions());
        assertEquals("single_key", ((KeyGroupedPartitioning) deserialized).keys()[0].getKey());
        assertEquals(
                "single_value",
                ((KeyGroupedPartitioning) deserialized).getPartitionValues()[0].getField(0));
    }

    @Test
    public void testSerializeWithNumericStrings() throws IOException {
        // Test with string values that look like numbers
        TransformExpression[] keys = {
                new TransformExpression("code", null, null)
        };

        Row[] partitionValues = {
                Row.of("001"),
                Row.of("002"),
                Row.of("999")
        };

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(keys, partitionValues, 3);

        String json = PartitionSerializer.serialize(original);
        Partitioning deserialized = PartitionSerializer.deserialize(json);

        assertTrue(original.isCompatible((KeyGroupedPartitioning) deserialized));

        // Verify string values are preserved as strings
        Row[] deserializedValues = ((KeyGroupedPartitioning) deserialized).getPartitionValues();
        assertEquals("001", deserializedValues[0].getField(0));
        assertEquals("002", deserializedValues[1].getField(0));
        assertEquals("999", deserializedValues[2].getField(0));
    }

    @Test
    public void testJsonStructure() throws IOException {
        // Test that JSON contains expected structure
        TransformExpression[] keys = {
                new TransformExpression("test_key", "test_func", 42)
        };

        Row[] partitionValues = {
                Row.of("test_value", 123)
        };

        KeyGroupedPartitioning original = new KeyGroupedPartitioning(keys, partitionValues, 1);

        String json = PartitionSerializer.serialize(original);

        // Basic JSON structure validation
        assertTrue(json.contains("\"numPartitions\""));
        assertTrue(json.contains("\"keys\""));
        assertTrue(json.contains("\"partitionValues\""));
        assertTrue(json.contains("\"key\":\"test_key\""));
        assertTrue(json.contains("\"functionName\":\"test_func\""));
        assertTrue(json.contains("\"numBuckets\":42"));
        assertTrue(json.contains("\"test_value\""));
        assertTrue(json.contains("123"));
    }

    @Test
    public void testSerializeUnsupportedPartitioningType() {
        // Test that unsupported partitioning types throw IllegalArgumentException
        Partitioning unsupportedPartitioning = new Partitioning() {
            @Override
            public int numPartitions() {
                return 1;
            }
        };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            PartitionSerializer.serialize(unsupportedPartitioning);
        });

        assertTrue(exception.getMessage().contains("Only KeyGroupedPartitioning is supported"));
    }
}
