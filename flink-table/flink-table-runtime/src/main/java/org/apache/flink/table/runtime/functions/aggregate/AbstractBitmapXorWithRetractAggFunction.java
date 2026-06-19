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
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Abstract base class for BITMAP_XOR_AGG and BITMAP_XOR_CARDINALITY_AGG with retraction. */
@Internal
public abstract class AbstractBitmapXorWithRetractAggFunction<T>
        extends BuiltInAggregateFunction<
                T, AbstractBitmapXorWithRetractAggFunction.BitmapXorWithRetractAccumulator> {

    private final transient DataType valueDataType;

    public AbstractBitmapXorWithRetractAggFunction(LogicalType valueType) {
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
        return DataTypes.STRUCTURED(
                BitmapXorWithRetractAccumulator.class,
                DataTypes.FIELD("bitmapCount", DataTypes.INT().notNull()),
                DataTypes.FIELD("bitmap", DataTypes.BITMAP().notNull()));
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    /** Accumulator for BITMAP_XOR_AGG and BITMAP_XOR_CARDINALITY_AGG with retraction. */
    public static class BitmapXorWithRetractAccumulator {

        public int bitmapCount = 0;
        public RoaringBitmapData bitmap = RoaringBitmapData.empty();

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            BitmapXorWithRetractAccumulator that = (BitmapXorWithRetractAccumulator) obj;
            return bitmapCount == that.bitmapCount && Objects.equals(bitmap, that.bitmap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bitmapCount, bitmap);
        }
    }

    @Override
    public BitmapXorWithRetractAccumulator createAccumulator() {
        return new BitmapXorWithRetractAccumulator();
    }

    public void resetAccumulator(BitmapXorWithRetractAccumulator acc) {
        acc.bitmapCount = 0;
        acc.bitmap.clear();
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    public void accumulate(BitmapXorWithRetractAccumulator acc, @Nullable Bitmap bitmap) {
        if (bitmap == null) {
            return;
        }

        acc.bitmapCount++;
        acc.bitmap.xor(bitmap);
    }

    public void retract(BitmapXorWithRetractAccumulator acc, @Nullable Bitmap bitmap) {
        if (bitmap == null) {
            return;
        }

        acc.bitmapCount--;
        acc.bitmap.xor(bitmap);
    }

    public void merge(
            BitmapXorWithRetractAccumulator acc, Iterable<BitmapXorWithRetractAccumulator> its) {
        for (BitmapXorWithRetractAccumulator other : its) {
            acc.bitmapCount += other.bitmapCount;
            acc.bitmap.xor(other.bitmap);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Sub-classes
    // --------------------------------------------------------------------------------------------

    /** Built-in BITMAP_XOR_AGG with retraction aggregate function that returns bitmap. */
    public static final class BitmapXorWithRetractAggFunction
            extends AbstractBitmapXorWithRetractAggFunction<Bitmap> {

        public BitmapXorWithRetractAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BITMAP();
        }

        @Override
        public Bitmap getValue(BitmapXorWithRetractAccumulator acc) {
            return acc.bitmapCount <= 0 ? null : RoaringBitmapData.from(acc.bitmap);
        }
    }

    /**
     * Built-in BITMAP_XOR_CARDINALITY_AGG with retraction aggregate function that returns
     * cardinality.
     */
    public static final class BitmapXorCardinalityWithRetractAggFunction
            extends AbstractBitmapXorWithRetractAggFunction<Long> {

        public BitmapXorCardinalityWithRetractAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BIGINT();
        }

        @Override
        public Long getValue(BitmapXorWithRetractAccumulator acc) {
            return acc.bitmapCount <= 0 ? null : acc.bitmap.getLongCardinality();
        }
    }
}
