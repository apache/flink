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
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.types.logical.ArrayType;

import java.util.ArrayList;
import java.util.List;

/**
 * Converter for ListView state.
 *
 * <p>Converts between external ListView objects and internal ArrayData representation.
 */
@Internal
class ListViewStateConverter implements StateConverter {

    private final DataStructureConverter<Object, Object> elementConverter;
    private final ArrayData.ElementGetter elementGetter;

    ListViewStateConverter(
            ArrayType arrayType, DataStructureConverter<Object, Object> elementConverter) {
        this.elementConverter = elementConverter;
        this.elementGetter = ArrayData.createElementGetter(arrayType.getElementType());
    }

    @Override
    public Object toInternal(Object external) {
        ListView<?> listView = (ListView<?>) external;
        List<?> elements = listView.getList();

        Object[] internalArray = new Object[elements.size()];
        for (int i = 0; i < elements.size(); i++) {
            internalArray[i] = elementConverter.toInternal(elements.get(i));
        }
        return new GenericArrayData(internalArray);
    }

    @Override
    public Object toExternal(Object internal) {
        ArrayData arrayData = (ArrayData) internal;
        ListView<Object> listView = new ListView<>();

        List<Object> elements = new ArrayList<>();
        for (int i = 0; i < arrayData.size(); i++) {
            Object internalElement = elementGetter.getElementOrNull(arrayData, i);
            Object externalElement = elementConverter.toExternal(internalElement);
            elements.add(externalElement);
        }
        listView.setList(elements);
        return listView;
    }

    @Override
    public Object createNewInternalState() {
        return new GenericArrayData(new Object[0]);
    }
}
