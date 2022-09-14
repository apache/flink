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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Type information that wraps a serializer that originated from a {@link LogicalType}.
 *
 * <p>{@link TypeInformation} is a legacy class for the sole purpose of creating a {@link
 * TypeSerializer}. Instances of {@link TypeInformation} are not required in the table ecosystem but
 * sometimes enforced by interfaces of other modules (such as {@link
 * org.apache.flink.api.dag.Transformation}). Therefore, this class acts as an adapter whenever type
 * information is required.
 *
 * <p>Use {@link #of(LogicalType)} for type information of internal data structures.
 *
 * <p>Note: Instances of this class should only be created for passing it to interfaces that require
 * type information. This class should not be used as a replacement for a {@link LogicalType}.
 * Information such as the arity of a row type, field types, field names, etc. should be derived
 * from the {@link LogicalType} directly.
 *
 * <p>The original {@link LogicalType} is stored in every instance (for access during pre-flight
 * phase and planning) but has no effect on equality because only serialization matters during
 * runtime.
 *
 * @param <T> internal data structure
 */
@Internal
public final class InternalTypeInfo<T> extends TypeInformation<T> implements DataTypeQueryable {

    private static final String FORMAT = "%s(%s, %s)";

    private final LogicalType type;

    private final Class<T> typeClass;

    private final TypeSerializer<T> typeSerializer;

    private InternalTypeInfo(
            LogicalType type, Class<T> typeClass, TypeSerializer<T> typeSerializer) {
        this.type = Preconditions.checkNotNull(type);
        this.typeClass = Preconditions.checkNotNull(typeClass);
        this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
    }

    /**
     * Creates type information for a {@link LogicalType} that is represented by internal data
     * structures.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> InternalTypeInfo<T> of(LogicalType type) {
        final Class<?> typeClass = LogicalTypeUtils.toInternalConversionClass(type);
        final TypeSerializer<?> serializer = InternalSerializers.create(type);
        return (InternalTypeInfo<T>) new InternalTypeInfo(type, typeClass, serializer);
    }

    /** Creates type information for a {@link RowType} represented by internal data structures. */
    public static InternalTypeInfo<RowData> of(RowType type) {
        return of((LogicalType) type);
    }

    /** Creates type information for {@link RowType} represented by internal data structures. */
    public static InternalTypeInfo<RowData> ofFields(LogicalType... fieldTypes) {
        return of(RowType.of(fieldTypes));
    }

    /** Creates type information for {@link RowType} represented by internal data structures. */
    public static InternalTypeInfo<RowData> ofFields(
            LogicalType[] fieldTypes, String[] fieldNames) {
        return of(RowType.of(fieldTypes, fieldNames));
    }

    // --------------------------------------------------------------------------------------------
    // Internal methods for common tasks
    // --------------------------------------------------------------------------------------------

    public LogicalType toLogicalType() {
        return type;
    }

    public TypeSerializer<T> toSerializer() {
        return typeSerializer;
    }

    public RowType toRowType() {
        return (RowType) type;
    }

    public RowDataSerializer toRowSerializer() {
        return (RowDataSerializer) typeSerializer;
    }

    /**
     * @deprecated {@link TypeInformation} should just be a thin wrapper of a serializer. This
     *     method only exists for legacy code. It is recommended to use the {@link RowType} instead
     *     for logical operations.
     */
    @Deprecated
    public LogicalType[] toRowFieldTypes() {
        return toRowType().getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
    }

    /**
     * @deprecated {@link TypeInformation} should just be a thin wrapper of a serializer. This
     *     method only exists for legacy code. It is recommended to use the {@link RowType} instead
     *     for logical operations.
     */
    @Deprecated
    public String[] toRowFieldNames() {
        return toRowType().getFields().stream()
                .map(RowType.RowField::getName)
                .toArray(String[]::new);
    }

    /**
     * @deprecated {@link TypeInformation} should just be a thin wrapper of a serializer. This
     *     method only exists for legacy code. It is recommended to use the {@link RowType} instead
     *     for logical operations.
     */
    @Deprecated
    public int toRowSize() {
        return toRowType().getFieldCount();
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return DataTypeUtils.toInternalDataType(type);
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
    public Class<T> getTypeClass() {
        return typeClass;
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
                type.asSummaryString(),
                typeClass.getName(),
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
        final InternalTypeInfo<?> that = (InternalTypeInfo<?>) o;
        return typeSerializer.equals(that.typeSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeSerializer);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof InternalTypeInfo;
    }
}
