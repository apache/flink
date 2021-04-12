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

package org.apache.flink.table.types.inference.transforms;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

import javax.annotation.Nullable;

/**
 * Transformation that applies {@link TypeInfoDataTypeConverter} on {@link
 * LegacyTypeInformationType}.
 */
@Internal
public class LegacyToNonLegacyTransformation implements TypeTransformation {

    public static final TypeTransformation INSTANCE = new LegacyToNonLegacyTransformation();

    @Override
    public DataType transform(DataType typeToTransform) {
        return transform(null, typeToTransform);
    }

    @Override
    public DataType transform(@Nullable DataTypeFactory factory, DataType dataType) {
        if (factory == null) {
            throw new TableException(
                    "LegacyToNonLegacyTransformation requires access to the data type factory.");
        }
        final LogicalType type = dataType.getLogicalType();
        if (type instanceof LegacyTypeInformationType) {
            return TypeInfoDataTypeConverter.toDataType(
                    factory, ((LegacyTypeInformationType<?>) type).getTypeInformation(), true);
        }
        return dataType;
    }
}
