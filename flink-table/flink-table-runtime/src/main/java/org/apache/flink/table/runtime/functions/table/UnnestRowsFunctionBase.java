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

/** Base class for flattening ARRAY, MAP, and MULTISET using a table function. */
@Internal
public abstract class UnnestRowsFunctionBase extends BuiltInSpecializedFunction {

    public UnnestRowsFunctionBase() {
        super(BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS);
    }

    @Override
    public UserDefinedFunction specialize(SpecializedContext context) {
        final LogicalType argType =
                context.getCallContext().getArgumentDataTypes().get(0).getLogicalType();
        switch (argType.getTypeRoot()) {
            case ARRAY:
                final ArrayType arrayType = (ArrayType) argType;
                return createCollectionUnnestFunction(
                        context,
                        arrayType.getElementType(),
                        ArrayData.createElementGetter(arrayType.getElementType()));
            case MULTISET:
                final MultisetType multisetType = (MultisetType) argType;
                return createCollectionUnnestFunction(
                        context,
                        multisetType.getElementType(),
                        ArrayData.createElementGetter(multisetType.getElementType()));
            case MAP:
                final MapType mapType = (MapType) argType;
                return createMapUnnestFunction(
                        context,
                        RowType.of(false, mapType.getKeyType(), mapType.getValueType()),
                        ArrayData.createElementGetter(mapType.getKeyType()),
                        ArrayData.createElementGetter(mapType.getValueType()));
            default:
                throw new UnsupportedOperationException("Unsupported type for UNNEST: " + argType);
        }
    }

    protected abstract UserDefinedFunction createCollectionUnnestFunction(
            SpecializedContext context,
            LogicalType elementType,
            ArrayData.ElementGetter elementGetter);

    protected abstract UserDefinedFunction createMapUnnestFunction(
            SpecializedContext context,
            RowType keyValTypes,
            ArrayData.ElementGetter keyGetter,
            ArrayData.ElementGetter valueGetter);

    public static LogicalType getUnnestedType(LogicalType logicalType, boolean withOrdinality) {
        LogicalType baseType;
        switch (logicalType.getTypeRoot()) {
            case ARRAY:
                baseType = ((ArrayType) logicalType).getElementType();
                break;
            case MULTISET:
                baseType = ((MultisetType) logicalType).getElementType();
                break;
            case MAP:
                MapType mapType = (MapType) logicalType;
                if (withOrdinality) {
                    return RowType.of(
                            false,
                            new LogicalType[] {
                                mapType.getKeyType(),
                                mapType.getValueType(),
                                DataTypes.INT().notNull().getLogicalType()
                            },
                            new String[] {"f0", "f1", "ordinality"});
                }
                return RowType.of(false, mapType.getKeyType(), mapType.getValueType());
            default:
                throw new UnsupportedOperationException("Unsupported UNNEST type: " + logicalType);
        }

        if (withOrdinality) {
            return RowType.of(
                    false,
                    new LogicalType[] {baseType, DataTypes.INT().notNull().getLogicalType()},
                    new String[] {"f0", "ordinality"});
        }
        return baseType;
    }

    // --------------------------------------------------------------------------------------------
    // Runtime Implementation Base Classes
    // --------------------------------------------------------------------------------------------

    /** Base class for table functions that unwrap collections and maps. */
    protected abstract static class UnnestTableFunctionBase extends BuiltInTableFunction<Object> {
        private final transient DataType outputDataType;

        UnnestTableFunctionBase(SpecializedContext context, LogicalType elementType) {
            super(BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS, context);
            outputDataType = DataTypes.of(elementType).toInternal();
        }

        // The output type in the context is already wrapped, however, the result of the function
        // is not. Therefore, we this function has to be implemented with the custom output type.
        @Override
        public DataType getOutputDataType() {
            return outputDataType;
        }

        protected void evalArrayData(
                ArrayData arrayData,
                ArrayData.ElementGetter elementGetter,
                UnnestCollector collector) {
            if (arrayData == null) {
                return;
            }
            final int size = arrayData.size();
            for (int pos = 0; pos < size; pos++) {
                collector.collect(elementGetter.getElementOrNull(arrayData, pos), pos + 1);
            }
        }

        protected void evalMapData(
                MapData mapData,
                ArrayData.ElementGetter keyGetter,
                ArrayData.ElementGetter valueGetter,
                MapUnnestCollector collector) {
            if (mapData == null) {
                return;
            }
            final int size = mapData.size();
            final ArrayData keyArray = mapData.keyArray();
            final ArrayData valueArray = mapData.valueArray();
            for (int i = 0; i < size; i++) {
                collector.collect(
                        keyGetter.getElementOrNull(keyArray, i),
                        valueGetter.getElementOrNull(valueArray, i),
                        i + 1);
            }
        }

        protected void evalMultisetData(
                MapData mapData, ArrayData.ElementGetter elementGetter, UnnestCollector collector) {
            if (mapData == null) {
                return;
            }
            final int size = mapData.size();
            final ArrayData keys = mapData.keyArray();
            final ArrayData values = mapData.valueArray();
            int ordinal = 1;
            for (int pos = 0; pos < size; pos++) {
                final int multiplier = values.getInt(pos);
                final Object key = elementGetter.getElementOrNull(keys, pos);
                for (int i = 0; i < multiplier; i++) {
                    collector.collect(key, ordinal++);
                }
            }
        }

        @FunctionalInterface
        protected interface UnnestCollector {
            void collect(Object element, int position);
        }

        @FunctionalInterface
        protected interface MapUnnestCollector {
            void collect(Object key, Object value, int position);
        }
    }
}
