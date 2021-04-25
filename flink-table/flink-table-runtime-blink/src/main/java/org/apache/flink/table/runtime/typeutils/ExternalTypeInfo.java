/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Type information that wraps a serializer that originated from a {@link DataType}.
 *
 * <p>{@link TypeInformation} is a legacy class for the sole purpose of creating a {@link
 * TypeSerializer}. This class acts as an adapter when entering or leaving the table ecosystem to
 * other APIs where type information is required.
 *
 * <p>The original {@link DataType} is stored in every instance (for access during pre-flight phase
 * and planning) but has no effect on equality because only serialization matters during runtime.
 *
 * <p>Note: This class is incomplete yet and only supports RAW types. But will be updated to support
 * all kinds of data types in the future.
 *
 * @param <T> external data structure
 */
@Internal
public final class ExternalTypeInfo<T> extends TypeInformation<T> implements DataTypeQueryable {

    private static final String FORMAT = "%s(%s, %s)";

    private final DataType dataType;

    private final TypeSerializer<T> typeSerializer;

    private ExternalTypeInfo(DataType dataType, TypeSerializer<T> typeSerializer) {
        this.dataType = Preconditions.checkNotNull(dataType);
        this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
    }

    /**
     * Creates type information for a {@link DataType} that is possibly represented by internal data
     * structures but serialized and deserialized into external data structures.
     */
    public static <T> ExternalTypeInfo<T> of(DataType dataType) {
        final TypeSerializer<T> serializer = createExternalTypeSerializer(dataType, false);
        return new ExternalTypeInfo<>(dataType, serializer);
    }

    /**
     * Creates type information for a {@link DataType} that is possibly represented by internal data
     * structures but serialized and deserialized into external data structures.
     *
     * @param isInternalInput allows for a non-bidirectional serializer from internal to external
     */
    public static <T> ExternalTypeInfo<T> of(DataType dataType, boolean isInternalInput) {
        final TypeSerializer<T> serializer =
                createExternalTypeSerializer(dataType, isInternalInput);
        return new ExternalTypeInfo<>(dataType, serializer);
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeSerializer<T> createExternalTypeSerializer(
            DataType dataType, boolean isInternalInput) {
        final LogicalType logicalType = dataType.getLogicalType();
        if (logicalType instanceof RawType && !isInternalInput) {
            final RawType<?> rawType = (RawType<?>) logicalType;
            if (dataType.getConversionClass() == rawType.getOriginatingClass()) {
                return (TypeSerializer<T>) rawType.getTypeSerializer();
            }
        }
        // note: we can add more special cases in the future to make the serialization more
        // efficient, for example we can translate to RowTypeInfo if we know that field types can
        // be mapped to type information as well, the external serializer in its current shape is
        // the most general serializer implementation for all conversion classes
        return ExternalSerializer.of(dataType, isInternalInput);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return dataType;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<T> getTypeClass() {
        return (Class<T>) dataType.getConversionClass();
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return typeSerializer;
    }

    @Override
    public String toString() {
        return String.format(
                FORMAT,
                dataType.getLogicalType().asSummaryString(),
                dataType.getConversionClass().getName(),
                typeSerializer.getClass().getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExternalTypeInfo<?> that = (ExternalTypeInfo<?>) o;
        return typeSerializer.equals(that.typeSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeSerializer);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof ExternalTypeInfo;
    }
}
