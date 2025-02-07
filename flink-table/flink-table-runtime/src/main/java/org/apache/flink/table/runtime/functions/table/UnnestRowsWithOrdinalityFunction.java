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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * Flattens ARRAY, MAP, and MULTISET using a table function and adds one extra column with the
 * position of the element. It does this by another level of specialization using a subclass of
 * {@link UnnestTableFunctionBase}.
 */
@Internal
public class UnnestRowsWithOrdinalityFunction extends UnnestRowsFunctionBase {

    public UnnestRowsWithOrdinalityFunction() {
        super();
    }

    @Override
    protected UserDefinedFunction createCollectionUnnestFunction(
            SpecializedContext context,
            LogicalType elementType,
            ArrayData.ElementGetter elementGetter) {
        return new CollectionUnnestWithOrdinalityFunction(context, elementType, elementGetter);
    }

    @Override
    protected UserDefinedFunction createMapUnnestFunction(
            SpecializedContext context,
            RowType keyValTypes,
            ArrayData.ElementGetter keyGetter,
            ArrayData.ElementGetter valueGetter) {
        return new MapUnnestWithOrdinalityFunction(context, keyValTypes, keyGetter, valueGetter);
    }

    /**
     * Table function that unwraps the elements of a collection (array or multiset) with ordinality.
     */
    public static final class CollectionUnnestWithOrdinalityFunction
            extends UnnestTableFunctionBase {
        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter elementGetter;
        private final transient DataType outputDataType;
        private RowData.FieldGetter[] fieldGetters = null;

        public CollectionUnnestWithOrdinalityFunction(
                SpecializedContext context,
                LogicalType elementType,
                ArrayData.ElementGetter elementGetter) {
            super(context, elementType);
            this.elementGetter = elementGetter;

            if (elementType instanceof RowType) {
                RowType rowType = (RowType) elementType;
                this.fieldGetters = createFieldGetters(rowType);
                this.outputDataType = createRowTypeOutputDataType(rowType);
            } else {
                this.outputDataType = createSimpleOutputDataType(elementType);
            }
        }

        @Override
        public DataType getOutputDataType() {
            return outputDataType;
        }

        public void eval(ArrayData arrayData) {
            evalArrayData(arrayData, elementGetter, this::collectWithOrdinality);
        }

        public void eval(MapData mapData) {
            evalMultisetData(mapData, elementGetter, this::collectWithOrdinality);
        }

        private void collectWithOrdinality(Object element, int position) {
            if (element instanceof RowData) {
                RowData innerRow = (RowData) element;
                int arity = innerRow.getArity();
                GenericRowData outRow = new GenericRowData(arity + 1);

                for (int i = 0; i < arity; i++) {
                    outRow.setField(i, fieldGetters[i].getFieldOrNull(innerRow));
                }

                outRow.setField(arity, position);
                collect(outRow);
            } else {
                collect(GenericRowData.of(element, position));
            }
        }

        private RowData.FieldGetter[] createFieldGetters(RowType rowType) {
            int fieldCount = rowType.getFieldCount();
            RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                fieldGetters[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i);
            }
            return fieldGetters;
        }

        private DataType createRowTypeOutputDataType(RowType rowType) {
            int fieldCount = rowType.getFieldCount();
            List<DataTypes.Field> fields = new ArrayList<>();
            for (int i = 0; i < fieldCount; i++) {
                fields.add(DataTypes.FIELD("f" + i, DataTypes.of(rowType.getTypeAt(i))));
            }
            fields.add(DataTypes.FIELD("ordinality", DataTypes.INT().notNull()));
            return DataTypes.ROW(fields).toInternal();
        }

        private DataType createSimpleOutputDataType(LogicalType elementType) {
            return DataTypes.ROW(
                            DataTypes.FIELD("f0", DataTypes.of(elementType)),
                            DataTypes.FIELD("ordinality", DataTypes.INT().notNull()))
                    .toInternal();
        }
    }

    /** Table function that unwraps the elements of a map with ordinality. */
    public static final class MapUnnestWithOrdinalityFunction extends UnnestTableFunctionBase {

        private static final long serialVersionUID = 1L;

        private final ArrayData.ElementGetter keyGetter;
        private final ArrayData.ElementGetter valueGetter;

        private final transient DataType outputDataType;

        public MapUnnestWithOrdinalityFunction(
                SpecializedContext context,
                LogicalType keyValTypes,
                ArrayData.ElementGetter keyGetter,
                ArrayData.ElementGetter valueGetter) {
            super(context, keyValTypes);
            this.keyGetter = keyGetter;
            this.valueGetter = valueGetter;

            RowType rowType = (RowType) keyValTypes;
            outputDataType =
                    DataTypes.ROW(
                                    DataTypes.FIELD("f0", DataTypes.of(rowType.getTypeAt(0))),
                                    DataTypes.FIELD("f1", DataTypes.of(rowType.getTypeAt(1))),
                                    DataTypes.FIELD("ordinality", DataTypes.INT().notNull()))
                            .toInternal();
        }

        @Override
        public DataType getOutputDataType() {
            return outputDataType;
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
