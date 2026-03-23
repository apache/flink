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

/** Built-in BITMAP_XOR_AGG aggregate function. */
@Internal
public final class BitmapXorAggFunction
        extends BuiltInAggregateFunction<Bitmap, BitmapXorAggFunction.BitmapXorAccumulator> {

    private final transient DataType valueDataType;

    public BitmapXorAggFunction(LogicalType valueType) {
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
                BitmapXorAccumulator.class, DataTypes.FIELD("bitmap", DataTypes.BITMAP()));
    }

    @Override
    public DataType getOutputDataType() {
        return DataTypes.BITMAP();
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    /** Accumulator for BITMAP_XOR_AGG. */
    public static class BitmapXorAccumulator {

        public @Nullable RoaringBitmapData bitmap;

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            BitmapXorAggFunction.BitmapXorAccumulator that =
                    (BitmapXorAggFunction.BitmapXorAccumulator) obj;
            return Objects.equals(bitmap, that.bitmap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bitmap);
        }
    }

    @Override
    public BitmapXorAccumulator createAccumulator() {
        return new BitmapXorAccumulator();
    }

    public void resetAccumulator(BitmapXorAccumulator acc) {
        acc.bitmap = null;
    }

    @Override
    public Bitmap getValue(BitmapXorAccumulator acc) {
        return Bitmap.from(acc.bitmap);
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    public void accumulate(BitmapXorAccumulator acc, @Nullable Bitmap bitmap) {
        if (bitmap == null) {
            return;
        }

        if (acc.bitmap != null) {
            acc.bitmap.xor(bitmap);
        } else {
            acc.bitmap = RoaringBitmapData.from(bitmap);
        }
    }

    public void merge(BitmapXorAccumulator acc, Iterable<BitmapXorAccumulator> its) {
        for (BitmapXorAccumulator other : its) {
            if (other.bitmap != null) {
                if (acc.bitmap != null) {
                    acc.bitmap.xor(other.bitmap);
                } else {
                    acc.bitmap = RoaringBitmapData.from(other.bitmap);
                }
            }
        }
    }
}
