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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Placeholder type of an arbitrary serialized type backed by {@link TypeInformation}. This type is
 * a black box within the table ecosystem and is only deserialized at the edges. The raw type is an
 * extension to the SQL standard.
 *
 * <p>Compared to an {@link RawType}, this type does not contain a {@link TypeSerializer} yet. The
 * serializer will be generated from the enclosed {@link TypeInformation} but needs access to the
 * {@link ExecutionConfig} of the current execution environment. Thus, this type is just a
 * placeholder for the fully resolved {@link RawType} returned by {@link #resolve(ExecutionConfig)}.
 *
 * <p>This type has no serializable string representation.
 *
 * <p>If no type information is supplied, generic type serialization for {@link Object} is used.
 *
 * @deprecated Use {@link RawType} instead.
 */
@PublicEvolving
@Deprecated
public final class TypeInformationRawType<T> extends LogicalType {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "RAW('%s', ?)";

    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(byte[].class.getName(), RawValueData.class.getName());

    private static final TypeInformation<?> DEFAULT_TYPE_INFO = Types.GENERIC(Object.class);

    private final TypeInformation<T> typeInfo;

    public TypeInformationRawType(boolean isNullable, TypeInformation<T> typeInfo) {
        super(isNullable, LogicalTypeRoot.RAW);
        this.typeInfo = Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
    }

    public TypeInformationRawType(TypeInformation<T> typeInfo) {
        this(true, typeInfo);
    }

    @SuppressWarnings("unchecked")
    public TypeInformationRawType() {
        this(true, (TypeInformation<T>) DEFAULT_TYPE_INFO);
    }

    public TypeInformation<T> getTypeInformation() {
        return typeInfo;
    }

    @Internal
    public RawType<T> resolve(ExecutionConfig config) {
        return new RawType<>(
                isNullable(), typeInfo.getTypeClass(), typeInfo.createSerializer(config));
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new TypeInformationRawType<>(
                isNullable, typeInfo); // we must assume immutability here
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, typeInfo.getTypeClass().getName());
    }

    @Override
    public String asSerializableString() {
        throw new TableException(
                "A raw type backed by type information has no serializable string representation. It "
                        + "needs to be resolved into a proper raw type.");
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return typeInfo.getTypeClass().isAssignableFrom(clazz)
                || INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return clazz.isAssignableFrom(typeInfo.getTypeClass())
                || INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return typeInfo.getTypeClass();
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TypeInformationRawType<?> that = (TypeInformationRawType<?>) o;
        return typeInfo.equals(that.typeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), typeInfo);
    }
}
