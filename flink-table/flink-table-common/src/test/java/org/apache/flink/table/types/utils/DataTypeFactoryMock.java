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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.extraction.DataTypeExtractor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link DataTypeFactory} mock for testing purposes. */
public class DataTypeFactoryMock implements DataTypeFactory {

    public Optional<DataType> dataType = Optional.empty();

    public Optional<LogicalType> logicalType = Optional.empty();

    public Optional<Class<?>> expectedClass = Optional.empty();

    @Override
    public DataType createDataType(AbstractDataType<?> abstractDataType) {
        if (abstractDataType instanceof DataType) {
            return (DataType) abstractDataType;
        } else if (abstractDataType instanceof UnresolvedDataType) {
            return ((UnresolvedDataType) abstractDataType).toDataType(this);
        }
        throw new IllegalStateException();
    }

    @Override
    public DataType createDataType(String typeString) {
        return TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(typeString));
    }

    @Override
    public DataType createDataType(UnresolvedIdentifier identifier) {
        return dataType.orElseThrow(() -> new ValidationException("No type found."));
    }

    @Override
    public <T> DataType createDataType(Class<T> clazz) {
        expectedClass.ifPresent(expected -> assertThat(expected).isEqualTo(clazz));
        return DataTypeExtractor.extractFromType(this, clazz);
    }

    @Override
    public <T> DataType createDataType(TypeInformation<T> typeInfo) {
        return TypeInfoDataTypeConverter.toDataType(this, typeInfo);
    }

    @Override
    public <T> DataType createRawDataType(Class<T> clazz) {
        expectedClass.ifPresent(expected -> assertThat(expected).isEqualTo(clazz));
        return dataType.orElseThrow(IllegalStateException::new);
    }

    @Override
    public <T> DataType createRawDataType(TypeInformation<T> typeInfo) {
        return dataType.orElseThrow(IllegalStateException::new);
    }

    @Override
    public LogicalType createLogicalType(String typeString) {
        return logicalType.orElseThrow(() -> new ValidationException("No type found."));
    }

    @Override
    public LogicalType createLogicalType(UnresolvedIdentifier identifier) {
        return logicalType.orElseThrow(() -> new ValidationException("No type found."));
    }

    /** Simulates a RAW type. */
    public static DataType dummyRaw(Class<?> clazz) {
        return DataTypes.RAW(clazz, dummySerializer());
    }

    /** Simulates a serializer. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> TypeSerializer<T> dummySerializer() {
        return (TypeSerializer) VoidSerializer.INSTANCE;
    }
}
