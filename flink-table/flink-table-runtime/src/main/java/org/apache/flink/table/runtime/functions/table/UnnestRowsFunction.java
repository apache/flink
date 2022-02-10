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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.runtime.functions.BuiltInSpecializedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Flattens ARRAY, MAP, and MULTISET using a table function. It does this by another level of
 * specialization using a subclass of {@link UnnestTableFunctionBase}.
 */
@Internal
public class UnnestRowsFunction extends BuiltInSpecializedFunction {

    public UnnestRowsFunction() {
        super(BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS);
    }

    @Override
    public UserDefinedFunction specialize(SpecializedContext context) {
        final LogicalType argType =
                context.getCallContext().getArgumentDataTypes().get(0).getLogicalType();
        switch (argType.getTypeRoot()) {
            case ARRAY:
                final ArrayType arrayType = (ArrayType) argType;
                return new CollectionUnnestTableFunction(
                        context,
                        arrayType.getElementType(),
                        ArrayData.createElementGetter(arrayType.getElementType()));
            case MULTISET:
                final MultisetType multisetType = (MultisetType) argType;
                return new CollectionUnnestTableFunction(
                        context,
                        multisetType.getElementType(),
                        ArrayData.createElementGetter(multisetType.getElementType()));
            case MAP:
                final MapType mapType = (MapType) argType;
                return new MapUnnestTableFunction(
                        context,
                        RowType.of(false, mapType.getKeyType(), mapType.getValueType()),
                        ArrayData.createElementGetter(mapType.getKeyType()),
                        ArrayData.createElementGetter(mapType.getValueType()));
            default:
                throw new UnsupportedOperationException("Unsupported type for UNNEST: " + argType);
        }
    }

    public static LogicalType getUnnestedType(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case ARRAY:
                return ((ArrayType) logicalType).getElementType();
            case MULTISET:
                return ((MultisetType) logicalType).getElementType();
            case MAP:
                final MapType mapType = (MapType) logicalType;
                return RowType.of(false, mapType.getKeyType(), mapType.getValueType());
            default:
                throw new UnsupportedOperationException("Unsupported UNNEST type: " + logicalType);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Runtime Implementation
    // --------------------------------------------------------------------------------------------

    private abstract static class UnnestTableFunctionBase extends BuiltInTableFunction<Object> {

        private final transient DataType outputDataType;

        UnnestTableFunctionBase(SpecializedContext context, LogicalType outputType) {
            super(BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS, context);
            // The output type in the context is already wrapped, however, the result of the
            // function is not. Therefore, we need a custom output type.
            outputDataType = DataTypes.of(outputType).toInternal();
        }

        @Override
        public DataType getOutputDataType() {
            return outputDataType;
        }
    }

    /** Table function that unwraps the elements of a collection (array or multiset). */
    public static final class CollectionUnnestTableFunction extends UnnestTableFunctionBase {

        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter elementGetter;

        public CollectionUnnestTableFunction(
                SpecializedContext context,
                LogicalType outputType,
                ArrayData.ElementGetter elementGetter) {
            super(context, outputType);
            this.elementGetter = elementGetter;
        }

        public void eval(ArrayData arrayData) {
            if (arrayData == null) {
                return;
            }
            final int size = arrayData.size();
            for (int pos = 0; pos < size; pos++) {
                collect(elementGetter.getElementOrNull(arrayData, pos));
            }
        }

        public void eval(MapData mapData) {
            if (mapData == null) {
                return;
            }
            final int size = mapData.size();
            final ArrayData keys = mapData.keyArray();
            final ArrayData values = mapData.valueArray();
            for (int pos = 0; pos < size; pos++) {
                final int multiplier = values.getInt(pos);
                final Object key = elementGetter.getElementOrNull(keys, pos);
                for (int i = 0; i < multiplier; i++) {
                    collect(key);
                }
            }
        }
    }

    /** Table function that unwraps the elements of a map. */
    public static final class MapUnnestTableFunction extends UnnestTableFunctionBase {

        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter keyGetter;

        private final ArrayData.ElementGetter valueGetter;

        public MapUnnestTableFunction(
                SpecializedContext context,
                LogicalType outputType,
                ArrayData.ElementGetter keyGetter,
                ArrayData.ElementGetter valueGetter) {
            super(context, outputType);
            this.keyGetter = keyGetter;
            this.valueGetter = valueGetter;
        }

        public void eval(MapData mapData) {
            if (mapData == null) {
                return;
            }
            final int size = mapData.size();
            final ArrayData keyArray = mapData.keyArray();
            final ArrayData valueArray = mapData.valueArray();
            for (int i = 0; i < size; i++) {
                collect(
                        GenericRowData.of(
                                keyGetter.getElementOrNull(keyArray, i),
                                valueGetter.getElementOrNull(valueArray, i)));
            }
        }
    }
}
