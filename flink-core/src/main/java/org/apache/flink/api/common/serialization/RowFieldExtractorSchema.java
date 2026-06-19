/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Serialization schema that extracts a specific field from a {@link Row} and returns it as a byte
 * array.
 *
 * <p>The field is required to be of type {@code byte[]}. This schema is particularly useful when
 * using Flink with Kafka, where you may want to use one Row field as the message key and another as
 * the value and perform the conversion to bytes explicitly in user code.
 *
 * <p>Example usage with Kafka:
 *
 * <pre>{@code
 * KafkaSink<Row> sink = KafkaSink.<Row>builder()
 *     .setBootstrapServers(bootstrapServers)
 *     .setRecordSerializer(
 *         KafkaRecordSerializationSchema.builder()
 *             .setTopic("my-topic")
 *             .setKeySerializationSchema(new RowFieldExtractorSchema(0))    // field 0 as key
 *             .setValueSerializationSchema(new RowFieldExtractorSchema(1))  // field 1 as value
 *             .build())
 *     .build();
 * }</pre>
 */
@PublicEvolving
public class RowFieldExtractorSchema implements SerializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    /** The index of the field to extract from the Row. */
    private final int fieldIndex;

    /**
     * Creates a new RowFieldExtractorSchema that extracts the field at the specified index.
     *
     * @param fieldIndex the zero-based index of the field to extract
     * @throws IllegalArgumentException if fieldIndex is negative
     */
    public RowFieldExtractorSchema(int fieldIndex) {
        checkArgument(fieldIndex >= 0, "Field index must be non-negative, got: %s", fieldIndex);
        this.fieldIndex = fieldIndex;
    }

    /**
     * Gets the field index being extracted.
     *
     * @return the field index
     */
    @VisibleForTesting
    public int getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public byte[] serialize(@Nullable Row element) {
        if (element == null) {
            return new byte[0];
        }

        checkArgument(
                fieldIndex < element.getArity(),
                "Cannot access field %s in Row with arity %s",
                fieldIndex,
                element.getArity());

        Object field = element.getField(fieldIndex);
        if (field == null) {
            return new byte[0];
        }

        if (!(field instanceof byte[])) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field at index %s must be of type byte[], but was %s",
                            fieldIndex, field.getClass().getName()));
        }

        return (byte[]) field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowFieldExtractorSchema that = (RowFieldExtractorSchema) o;
        return fieldIndex == that.fieldIndex;
    }

    @Override
    public int hashCode() {
        return fieldIndex;
    }
}
