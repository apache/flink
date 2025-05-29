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

package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Flattens ARRAY, MAP, and MULTISET using a table function and adds one extra column with the
 * position of the element. It does this by another level of specialization using a subclass of
 * {@link UnnestTableFunctionBase}.
 */
@Internal
public class UnnestRowsWithOrdinalityFunction extends UnnestRowsFunctionBase {

    public UnnestRowsWithOrdinalityFunction() {
        super(true);
    }

    @Override
    protected UserDefinedFunction createCollectionUnnestFunction(
            SpecializedContext context,
            LogicalType elementType,
            ArrayData.ElementGetter elementGetter) {
        return new CollectionUnnestWithOrdinalityFunction(
                context, wrapWithOrdinality(elementType), elementGetter);
    }

    @Override
    protected UserDefinedFunction createMapUnnestFunction(
            SpecializedContext context,
            RowType keyValTypes,
            ArrayData.ElementGetter keyGetter,
            ArrayData.ElementGetter valueGetter) {
        return new MapUnnestWithOrdinalityFunction(
                context, wrapWithOrdinality(keyValTypes), keyGetter, valueGetter);
    }

    /**
     * Table function that unwraps the elements of a collection (array or multiset) with ordinality.
     */
    public static final class CollectionUnnestWithOrdinalityFunction
            extends UnnestTableFunctionBase {
        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter elementGetter;

        public CollectionUnnestWithOrdinalityFunction(
                SpecializedContext context,
                LogicalType elementType,
                ArrayData.ElementGetter elementGetter) {
            super(context, elementType, true);
            this.elementGetter = elementGetter;
        }

        public void eval(ArrayData arrayData) {
            evalArrayData(arrayData, elementGetter, this::collectWithOrdinality);
        }

        public void eval(MapData mapData) {
            evalMultisetData(mapData, elementGetter, this::collectWithOrdinality);
        }

        private void collectWithOrdinality(Object element, int position) {
            if (element instanceof RowData) {
                RowData row = (RowData) element;
                collect(new JoinedRowData(row.getRowKind(), row, GenericRowData.of(position)));
            } else {
                collect(GenericRowData.of(element, position));
            }
        }
    }

    /** Table function that unwraps the elements of a map with ordinality. */
    public static final class MapUnnestWithOrdinalityFunction extends UnnestTableFunctionBase {

        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter keyGetter;
        private final ArrayData.ElementGetter valueGetter;

        public MapUnnestWithOrdinalityFunction(
                SpecializedContext context,
                LogicalType keyValTypes,
                ArrayData.ElementGetter keyGetter,
                ArrayData.ElementGetter valueGetter) {
            super(context, keyValTypes, true);
            this.keyGetter = keyGetter;
            this.valueGetter = valueGetter;
        }

        public void eval(MapData mapData) {
            evalMapData(
                    mapData,
                    keyGetter,
                    valueGetter,
                    (key, value, position) -> collect(GenericRowData.of(key, value, position)));
        }
    }
}
