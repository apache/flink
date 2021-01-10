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

package org.apache.flink.table.types.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Conversion hub for interoperability of {@link Class}, {@link TypeInformation}, {@link DataType},
 * and {@link LogicalType}.
 *
 * <p>See the corresponding converter classes for more information about how the conversion is
 * performed.
 */
@Internal
public final class TypeConversions {

    /**
     * @deprecated Please don't use this method anymore. It will be removed soon and we should not
     *     make the removal more painful.
     */
    @Deprecated
    public static DataType fromLegacyInfoToDataType(TypeInformation<?> typeInfo) {
        return LegacyTypeInfoDataTypeConverter.toDataType(typeInfo);
    }

    /**
     * @deprecated Please don't use this method anymore. It will be removed soon and we should not
     *     make the removal more painful.
     */
    @Deprecated
    public static DataType[] fromLegacyInfoToDataType(TypeInformation<?>[] typeInfo) {
        return Stream.of(typeInfo)
                .map(TypeConversions::fromLegacyInfoToDataType)
                .toArray(DataType[]::new);
    }

    /**
     * @deprecated Please don't use this method anymore. It will be removed soon and we should not
     *     make the removal more painful.
     */
    @Deprecated
    public static TypeInformation<?> fromDataTypeToLegacyInfo(DataType dataType) {
        return LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(dataType);
    }

    /**
     * @deprecated Please don't use this method anymore. It will be removed soon and we should not
     *     make the removal more painful.
     */
    @Deprecated
    public static TypeInformation<?>[] fromDataTypeToLegacyInfo(DataType[] dataType) {
        return Stream.of(dataType)
                .map(TypeConversions::fromDataTypeToLegacyInfo)
                .toArray(TypeInformation[]::new);
    }

    public static Optional<DataType> fromClassToDataType(Class<?> clazz) {
        return ClassDataTypeConverter.extractDataType(clazz);
    }

    public static DataType fromLogicalToDataType(LogicalType logicalType) {
        return LogicalTypeDataTypeConverter.toDataType(logicalType);
    }

    public static DataType[] fromLogicalToDataType(LogicalType[] logicalTypes) {
        return Stream.of(logicalTypes)
                .map(LogicalTypeDataTypeConverter::toDataType)
                .toArray(DataType[]::new);
    }

    public static LogicalType fromDataToLogicalType(DataType dataType) {
        return dataType.getLogicalType();
    }

    public static LogicalType[] fromDataToLogicalType(DataType[] dataTypes) {
        return Stream.of(dataTypes)
                .map(TypeConversions::fromDataToLogicalType)
                .toArray(LogicalType[]::new);
    }

    private TypeConversions() {
        // no instance
    }
}
