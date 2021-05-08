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

package org.apache.flink.table.runtime.types;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Optional;

/**
 * Utils for get {@link DataType} from a Class. It must return a DataType. Convert known types by
 * {@link TypeConversions#fromClassToDataType}. Convert unknown types by {@link
 * LegacyTypeInformationType}.
 */
public class ClassDataTypeConverter {

    /**
     * @param clazz The class of the type.
     * @return The DataType object for the type described by the hint.
     * @throws InvalidTypesException Cannot extract TypeInformation from Class alone, because
     *     generic parameters are missing.
     */
    public static DataType fromClassToDataType(Class<?> clazz) {
        Optional<DataType> optional = TypeConversions.fromClassToDataType(clazz);
        return optional.orElseGet(
                () ->
                        TypeConversions.fromLegacyInfoToDataType(
                                TypeExtractor.createTypeInfo(clazz)));
    }
}
