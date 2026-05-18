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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.extraction.DataTypeExtractor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

/**
 * {@link DataTypeFactory} implementation for {@link ProcessTableFunctionTestHarness}.
 *
 * <p>Only {@link #createDataType(AbstractDataType)}, {@link #createDataType(String)}, {@link
 * #createDataType(Class)}, and {@link #createDataType(TypeInformation)} are supported.
 */
@Internal
class TestHarnessDataTypeFactory implements DataTypeFactory {

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
        LogicalType logicalType =
                LogicalTypeParser.parse(typeString, Thread.currentThread().getContextClassLoader());
        return org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType(
                logicalType);
    }

    @Override
    public DataType createDataType(UnresolvedIdentifier identifier) {
        throw new UnsupportedOperationException(
                "TestHarnessDataTypeFactory does not support createDataType(UnresolvedIdentifier).");
    }

    @Override
    public <T> DataType createDataType(Class<T> clazz) {
        return DataTypeExtractor.extractFromType(this, clazz);
    }

    @Override
    public <T> DataType createDataType(TypeInformation<T> typeInfo) {
        return TypeInfoDataTypeConverter.toDataType(this, typeInfo);
    }

    @Override
    public <T> DataType createRawDataType(Class<T> clazz) {
        throw new UnsupportedOperationException(
                "TestHarnessDataTypeFactory does not support createRawDataType(Class).");
    }

    @Override
    public <T> DataType createRawDataType(TypeInformation<T> typeInfo) {
        throw new UnsupportedOperationException(
                "TestHarnessDataTypeFactory does not support createRawDataType(TypeInformation).");
    }

    @Override
    public LogicalType createLogicalType(String typeString) {
        throw new UnsupportedOperationException(
                "TestHarnessDataTypeFactory does not support createLogicalType(String).");
    }

    @Override
    public LogicalType createLogicalType(UnresolvedIdentifier identifier) {
        throw new UnsupportedOperationException(
                "TestHarnessDataTypeFactory does not support createLogicalType(UnresolvedIdentifier).");
    }
}
