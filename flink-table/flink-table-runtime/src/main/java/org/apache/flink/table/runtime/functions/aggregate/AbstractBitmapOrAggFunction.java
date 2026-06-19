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

/** Abstract base class for BITMAP_OR_AGG and BITMAP_OR_CARDINALITY_AGG. */
@Internal
public abstract class AbstractBitmapOrAggFunction<T>
        extends BuiltInAggregateFunction<T, AbstractBitmapOrAggFunction.BitmapOrAccumulator> {

    private final transient DataType valueDataType;

    public AbstractBitmapOrAggFunction(LogicalType valueType) {
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
                BitmapOrAccumulator.class, DataTypes.FIELD("bitmap", DataTypes.BITMAP()));
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    /** Accumulator for BITMAP_OR_AGG and BITMAP_OR_CARDINALITY_AGG. */
    public static class BitmapOrAccumulator {

        public @Nullable RoaringBitmapData bitmap;

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            BitmapOrAccumulator that = (BitmapOrAccumulator) obj;
            return Objects.equals(bitmap, that.bitmap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bitmap);
        }
    }

    @Override
    public BitmapOrAccumulator createAccumulator() {
        return new BitmapOrAccumulator();
    }

    public void resetAccumulator(BitmapOrAccumulator acc) {
        acc.bitmap = null;
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    public void accumulate(BitmapOrAccumulator acc, @Nullable Bitmap bitmap) {
        if (bitmap == null) {
            return;
        }

        if (acc.bitmap != null) {
            acc.bitmap.or(bitmap);
        } else {
            acc.bitmap = RoaringBitmapData.from(bitmap);
        }
    }

    public void merge(BitmapOrAccumulator acc, Iterable<BitmapOrAccumulator> its) {
        for (BitmapOrAccumulator other : its) {
            if (other.bitmap != null) {
                if (acc.bitmap != null) {
                    acc.bitmap.or(other.bitmap);
                } else {
                    acc.bitmap = RoaringBitmapData.from(other.bitmap);
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Sub-classes
    // --------------------------------------------------------------------------------------------

    /** Built-in BITMAP_OR_AGG aggregate function that returns bitmap. */
    public static final class BitmapOrAggFunction extends AbstractBitmapOrAggFunction<Bitmap> {

        public BitmapOrAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BITMAP();
        }

        @Override
        public Bitmap getValue(BitmapOrAccumulator acc) {
            return Bitmap.from(acc.bitmap);
        }
    }

    /** Built-in BITMAP_OR_CARDINALITY_AGG aggregate function that returns cardinality. */
    public static final class BitmapOrCardinalityAggFunction
            extends AbstractBitmapOrAggFunction<Long> {

        public BitmapOrCardinalityAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BIGINT();
        }

        @Override
        public Long getValue(BitmapOrAccumulator acc) {
            return acc.bitmap == null ? null : acc.bitmap.getLongCardinality();
        }
    }
}
