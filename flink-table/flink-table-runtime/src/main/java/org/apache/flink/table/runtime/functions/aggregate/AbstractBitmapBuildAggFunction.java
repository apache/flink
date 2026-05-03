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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.bitmap.Bitmap;
import org.apache.flink.types.bitmap.RoaringBitmapData;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Abstract base class for BITMAP_BUILD_AGG and BITMAP_BUILD_CARDINALITY_AGG. */
@Internal
public abstract class AbstractBitmapBuildAggFunction<T>
        extends BuiltInAggregateFunction<T, Bitmap> {

    private final transient DataType valueDataType;

    public AbstractBitmapBuildAggFunction(LogicalType valueType) {
        this.valueDataType = toInternalDataType(valueType);
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Collections.singletonList(valueDataType);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.BITMAP().notNull();
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    @Override
    public Bitmap createAccumulator() {
        return Bitmap.empty();
    }

    public void resetAccumulator(Bitmap acc) {
        acc.clear();
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    public void accumulate(Bitmap acc, @Nullable Integer value) {
        if (value != null) {
            acc.add(value);
        }
    }

    public void merge(Bitmap acc, Iterable<Bitmap> its) {
        for (Bitmap other : its) {
            acc.or(other);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Sub-classes
    // --------------------------------------------------------------------------------------------

    /** Built-in BITMAP_BUILD_AGG aggregate function that returns bitmap. */
    public static final class BitmapBuildAggFunction
            extends AbstractBitmapBuildAggFunction<Bitmap> {

        public BitmapBuildAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BITMAP();
        }

        @Override
        public Bitmap getValue(Bitmap acc) {
            return acc.isEmpty() ? null : RoaringBitmapData.from(acc);
        }
    }

    /** Built-in BITMAP_BUILD_CARDINALITY_AGG aggregate function that returns cardinality. */
    public static final class BitmapBuildCardinalityAggFunction
            extends AbstractBitmapBuildAggFunction<Long> {

        public BitmapBuildCardinalityAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BIGINT();
        }

        @Override
        public Long getValue(Bitmap acc) {
            return acc.isEmpty() ? null : acc.getLongCardinality();
        }
    }
}
