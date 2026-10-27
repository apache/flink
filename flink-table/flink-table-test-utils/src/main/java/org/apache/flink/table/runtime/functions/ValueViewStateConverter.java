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
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.data.conversion.DataStructureConverter;

/**
 * Converter for ValueView state.
 *
 * <p>Converts between external ValueView objects and the internal representation of the contained
 * value. An empty view is represented by a {@code null} internal value.
 */
@Internal
class ValueViewStateConverter implements StateConverter {

    private final DataStructureConverter<Object, Object> valueConverter;

    ValueViewStateConverter(DataStructureConverter<Object, Object> valueConverter) {
        this.valueConverter = valueConverter;
    }

    @Override
    public Object toInternal(Object external) {
        ValueView<?> valueView = (ValueView<?>) external;
        Object value = valueView.getValue();
        return value == null ? null : valueConverter.toInternal(value);
    }

    @Override
    public Object toExternal(Object internal) {
        ValueView<Object> valueView = new ValueView<>();
        if (internal != null) {
            valueView.setValue(valueConverter.toExternal(internal));
        }
        return valueView;
    }

    @Override
    public Object createNewInternalState() {
        // An empty value view is represented by a null internal value.
        return null;
    }
}
