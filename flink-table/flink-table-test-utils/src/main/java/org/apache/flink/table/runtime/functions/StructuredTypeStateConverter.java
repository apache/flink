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
import org.apache.flink.table.data.conversion.DataStructureConverter;

/**
 * Converter for value state backed by structured types.
 *
 * <p>Converts between external value state objects and internal RowData representation.
 */
@Internal
class StructuredTypeStateConverter implements StateConverter {

    private final DataStructureConverter<Object, Object> converter;
    private final Class<?> pojoClass;

    StructuredTypeStateConverter(
            Class<?> pojoClass, DataStructureConverter<Object, Object> converter) {
        this.converter = converter;
        this.pojoClass = pojoClass;
    }

    @Override
    public Object toInternal(Object external) {
        if (external == null) {
            return null;
        }
        return converter.toInternal(external);
    }

    @Override
    public Object toExternal(Object internal) {
        if (internal == null) {
            return null;
        }
        return converter.toExternal(internal);
    }

    @Override
    public Object createNewInternalState() {
        try {
            Object newPojo = pojoClass.getDeclaredConstructor().newInstance();
            return converter.toInternal(newPojo);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create new instance of POJO class: " + pojoClass.getName(), e);
        }
    }
}
