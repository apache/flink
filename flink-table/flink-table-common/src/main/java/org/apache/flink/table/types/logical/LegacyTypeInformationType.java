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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toInternalConversionClass;

/**
 * This type is a temporary solution to fully support the old type system stack through the new
 * stack. Many types can be mapped directly to the new type system, however, some types such as
 * {@code DECIMAL}, POJOs, or case classes need special handling.
 *
 * <p>This type differs from {@link TypeInformationRawType}. This type is allowed to travel through
 * the stack whereas {@link TypeInformationRawType} should be resolved eagerly to {@link RawType} by
 * the planner.
 *
 * <p>This class can be removed once we have removed all deprecated methods that take or return
 * {@link TypeInformation}.
 *
 * @see LegacyTypeInfoDataTypeConverter
 */
@Internal
public final class LegacyTypeInformationType<T> extends LogicalType {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "LEGACY('%s', '%s')";

    private final TypeInformation<T> typeInfo;

    public LegacyTypeInformationType(LogicalTypeRoot logicalTypeRoot, TypeInformation<T> typeInfo) {
        super(true, logicalTypeRoot);
        this.typeInfo = Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
    }

    public TypeInformation<T> getTypeInformation() {
        return typeInfo;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new LegacyTypeInformationType<>(getTypeRoot(), typeInfo);
    }

    @Override
    public String asSerializableString() {
        return withNullability(
                FORMAT,
                getTypeRoot(),
                EncodingUtils.escapeSingleQuotes(TypeStringUtils.writeTypeInfo(typeInfo)));
    }

    @Override
    public String asSummaryString() {
        return asSerializableString();
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return typeInfo.getTypeClass().isAssignableFrom(clazz)
                || clazz == toInternalConversionClass(this);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return clazz.isAssignableFrom(typeInfo.getTypeClass())
                || clazz == toInternalConversionClass(this);
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
        LegacyTypeInformationType<?> that = (LegacyTypeInformationType<?>) o;
        return typeInfo.equals(that.typeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), typeInfo);
    }
}
