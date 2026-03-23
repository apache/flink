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
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Abstract base class for BITMAP_OR_AGG and BITMAP_OR_CARDINALITY_AGG with retraction. */
@Internal
public abstract class AbstractBitmapOrWithRetractAggFunction<T>
        extends BuiltInAggregateFunction<
                T, AbstractBitmapOrWithRetractAggFunction.BitmapOrWithRetractAccumulator> {

    private final transient DataType valueDataType;

    public AbstractBitmapOrWithRetractAggFunction(LogicalType valueType) {
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
                BitmapOrWithRetractAccumulator.class,
                DataTypes.FIELD("bitmapCount", DataTypes.INT().notNull()),
                DataTypes.FIELD("bitmap", DataTypes.BITMAP().notNull()),
                DataTypes.FIELD(
                        "valueCount",
                        MapView.newMapViewDataType(
                                DataTypes.INT().notNull(), DataTypes.INT().notNull())));
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    /** Accumulator for BITMAP_OR_AGG and BITMAP_OR_CARDINALITY_AGG with retraction. */
    public static class BitmapOrWithRetractAccumulator {

        public int bitmapCount = 0;
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
            BitmapOrWithRetractAccumulator that = (BitmapOrWithRetractAccumulator) obj;
            return bitmapCount == that.bitmapCount
                    && Objects.equals(bitmap, that.bitmap)
                    && Objects.equals(valueCount, that.valueCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bitmapCount, bitmap, valueCount);
        }
    }

    @Override
    public BitmapOrWithRetractAccumulator createAccumulator() {
        return new BitmapOrWithRetractAccumulator();
    }

    public void resetAccumulator(BitmapOrWithRetractAccumulator acc) {
        acc.bitmapCount = 0;
        acc.bitmap.clear();
        acc.valueCount.clear();
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    public void accumulate(BitmapOrWithRetractAccumulator acc, @Nullable Bitmap bitmap)
            throws Exception {
        if (bitmap == null) {
            return;
        }

        acc.bitmapCount++;

        RoaringBitmapData.toRoaringBitmapData(bitmap)
                .forEach(
                        value -> {
                            try {
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
                            } catch (Exception e) {
                                throw new FlinkRuntimeException(e);
                            }
                        });
    }

    public void retract(BitmapOrWithRetractAccumulator acc, @Nullable Bitmap bitmap)
            throws Exception {
        if (bitmap == null) {
            return;
        }

        acc.bitmapCount--;

        RoaringBitmapData.toRoaringBitmapData(bitmap)
                .forEach(
                        value -> {
                            try {
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
                            } catch (Exception e) {
                                throw new FlinkRuntimeException(e);
                            }
                        });
    }

    public void merge(
            BitmapOrWithRetractAccumulator acc, Iterable<BitmapOrWithRetractAccumulator> its)
            throws Exception {
        for (BitmapOrWithRetractAccumulator other : its) {
            acc.bitmapCount += other.bitmapCount;
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

    /** Built-in BITMAP_OR_AGG with retraction aggregate function that returns bitmap. */
    public static final class BitmapOrWithRetractAggFunction
            extends AbstractBitmapOrWithRetractAggFunction<Bitmap> {

        public BitmapOrWithRetractAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BITMAP();
        }

        @Override
        public Bitmap getValue(BitmapOrWithRetractAccumulator acc) {
            return acc.bitmapCount <= 0 ? null : RoaringBitmapData.from(acc.bitmap);
        }
    }

    /**
     * Built-in BITMAP_OR_CARDINALITY_AGG with retraction aggregate function that returns
     * cardinality.
     */
    public static final class BitmapOrCardinalityWithRetractAggFunction
            extends AbstractBitmapOrWithRetractAggFunction<Long> {

        public BitmapOrCardinalityWithRetractAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BIGINT();
        }

        @Override
        public Long getValue(BitmapOrWithRetractAccumulator acc) {
            return acc.bitmapCount <= 0 ? null : acc.bitmap.getLongCardinality();
        }
    }
}
