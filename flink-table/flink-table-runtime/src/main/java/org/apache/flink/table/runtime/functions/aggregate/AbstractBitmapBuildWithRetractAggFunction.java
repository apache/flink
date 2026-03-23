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
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.bitmap.Bitmap;
import org.apache.flink.types.bitmap.RoaringBitmapData;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Abstract base class for BITMAP_BUILD_AGG and BITMAP_BUILD_CARDINALITY_AGG with retraction. */
@Internal
public abstract class AbstractBitmapBuildWithRetractAggFunction<T>
        extends BuiltInAggregateFunction<
                T, AbstractBitmapBuildWithRetractAggFunction.BitmapBuildWithRetractAccumulator> {

    private final transient DataType valueDataType;

    public AbstractBitmapBuildWithRetractAggFunction(LogicalType valueType) {
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
                BitmapBuildWithRetractAccumulator.class,
                DataTypes.FIELD("bitmap", DataTypes.BITMAP().notNull()),
                DataTypes.FIELD(
                        "valueCount",
                        MapView.newMapViewDataType(
                                DataTypes.INT().notNull(), DataTypes.INT().notNull())));
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    /** Accumulator for BITMAP_BUILD_AGG and BITMAP_BUILD_CARDINALITY_AGG with retraction. */
    public static class BitmapBuildWithRetractAccumulator {

        // bitmap should reflect the actual data based on valueCount
        public RoaringBitmapData bitmap = RoaringBitmapData.empty();
        public MapView<Integer, Integer> valueCount = new MapView<>();

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            BitmapBuildWithRetractAccumulator that = (BitmapBuildWithRetractAccumulator) obj;
            return Objects.equals(bitmap, that.bitmap)
                    && Objects.equals(valueCount, that.valueCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bitmap, valueCount);
        }
    }

    @Override
    public BitmapBuildWithRetractAccumulator createAccumulator() {
        return new BitmapBuildWithRetractAccumulator();
    }

    public void resetAccumulator(BitmapBuildWithRetractAccumulator acc) {
        acc.bitmap.clear();
        acc.valueCount.clear();
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    public void accumulate(BitmapBuildWithRetractAccumulator acc, @Nullable Integer value)
            throws Exception {
        if (value != null) {
            Integer count = acc.valueCount.get(value);
            count = count == null ? 1 : count + 1;

            if (count == 0) {
                acc.valueCount.remove(value);
            } else {
                acc.valueCount.put(value, count);
                // add value to bitmap if count changes from 0 to 1 or expires
                if (count == 1) {
                    acc.bitmap.add(value);
                }
            }
        }
    }

    public void retract(BitmapBuildWithRetractAccumulator acc, @Nullable Integer value)
            throws Exception {
        if (value != null) {
            Integer count = acc.valueCount.get(value);
            count = count == null ? -1 : count - 1;

            if (count == 0) {
                acc.valueCount.remove(value);
                // remove value from bitmap if count changes from 1 to 0
                acc.bitmap.remove(value);
            } else {
                acc.valueCount.put(value, count);
                if (count == -1) {
                    // remove value from bitmap if count expires
                    acc.bitmap.remove(value);
                }
            }
        }
    }

    public void merge(
            BitmapBuildWithRetractAccumulator acc, Iterable<BitmapBuildWithRetractAccumulator> its)
            throws Exception {
        for (BitmapBuildWithRetractAccumulator other : its) {
            for (Map.Entry<Integer, Integer> entry : other.valueCount.entries()) {
                Integer value = entry.getKey();
                // count != 0
                Integer count = entry.getValue();

                Integer curCount = acc.valueCount.get(value);
                curCount = curCount == null ? count : curCount + count;

                if (curCount == 0) {
                    acc.valueCount.remove(value);

                    if (curCount > count) {
                        // preCount > 0, value is in bitmap
                        acc.bitmap.remove(value);
                    }
                } else {
                    acc.valueCount.put(value, curCount);

                    if (0 < curCount && curCount <= count) {
                        // preCount < 0, value is not in bitmap
                        // preCount = 0, unknown (expiration)
                        acc.bitmap.add(value);
                    }

                    if (0 > curCount && curCount >= count) {
                        // preCount > 0, value is in bitmap
                        // preCount = 0, unknown (expiration)
                        acc.bitmap.remove(value);
                    }
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Sub-classes
    // --------------------------------------------------------------------------------------------

    /** Built-in BITMAP_BUILD_AGG with retraction aggregate function that returns bitmap. */
    public static final class BitmapBuildWithRetractAggFunction
            extends AbstractBitmapBuildWithRetractAggFunction<Bitmap> {

        public BitmapBuildWithRetractAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BITMAP();
        }

        @Override
        public Bitmap getValue(BitmapBuildWithRetractAccumulator acc) {
            return acc.bitmap.isEmpty() ? null : RoaringBitmapData.from(acc.bitmap);
        }
    }

    /**
     * Built-in BITMAP_BUILD_CARDINALITY_AGG with retraction aggregate function that returns
     * cardinality.
     */
    public static final class BitmapBuildCardinalityWithRetractAggFunction
            extends AbstractBitmapBuildWithRetractAggFunction<Long> {

        public BitmapBuildCardinalityWithRetractAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BIGINT();
        }

        @Override
        public Long getValue(BitmapBuildWithRetractAccumulator acc) {
            return acc.bitmap.isEmpty() ? null : acc.bitmap.getLongCardinality();
        }
    }
}
