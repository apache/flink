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

/** Abstract base class for BITMAP_AND_AGG and BITMAP_AND_CARDINALITY_AGG with retraction. */
@Internal
public abstract class AbstractBitmapAndWithRetractAggFunction<T>
        extends BuiltInAggregateFunction<
                T, AbstractBitmapAndWithRetractAggFunction.BitmapAndWithRetractAccumulator> {

    private final transient DataType valueDataType;

    public AbstractBitmapAndWithRetractAggFunction(LogicalType valueType) {
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
                BitmapAndWithRetractAccumulator.class,
                DataTypes.FIELD("bitmapCount", DataTypes.INT().notNull()),
                DataTypes.FIELD(
                        "valueCount",
                        MapView.newMapViewDataType(
                                DataTypes.INT().notNull(), DataTypes.INT().notNull())));
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    /** Accumulator for BITMAP_AND_AGG and BITMAP_AND_CARDINALITY_AGG with retraction. */
    public static class BitmapAndWithRetractAccumulator {

        public int bitmapCount = 0;
        public MapView<Integer, Integer> valueCount = new MapView<>();

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            BitmapAndWithRetractAccumulator that = (BitmapAndWithRetractAccumulator) obj;
            return bitmapCount == that.bitmapCount && Objects.equals(valueCount, that.valueCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bitmapCount, valueCount);
        }
    }

    @Override
    public BitmapAndWithRetractAccumulator createAccumulator() {
        return new BitmapAndWithRetractAccumulator();
    }

    public void resetAccumulator(BitmapAndWithRetractAccumulator acc) {
        acc.bitmapCount = 0;
        acc.valueCount.clear();
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    public void accumulate(BitmapAndWithRetractAccumulator acc, @Nullable Bitmap bitmap)
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
                                }
                            } catch (Exception e) {
                                throw new FlinkRuntimeException(e);
                            }
                        });
    }

    public void retract(BitmapAndWithRetractAccumulator acc, @Nullable Bitmap bitmap)
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
                                } else {
                                    acc.valueCount.put(value, count);
                                }
                            } catch (Exception e) {
                                throw new FlinkRuntimeException(e);
                            }
                        });
    }

    public void merge(
            BitmapAndWithRetractAccumulator acc, Iterable<BitmapAndWithRetractAccumulator> its)
            throws Exception {
        for (BitmapAndWithRetractAccumulator other : its) {
            acc.bitmapCount += other.bitmapCount;

            for (Map.Entry<Integer, Integer> entry : other.valueCount.entries()) {
                Integer value = entry.getKey();
                Integer count = entry.getValue();

                Integer curCount = acc.valueCount.get(value);
                curCount = curCount == null ? count : curCount + count;

                if (curCount == 0) {
                    acc.valueCount.remove(value);
                } else {
                    acc.valueCount.put(value, curCount);
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Sub-classes
    // --------------------------------------------------------------------------------------------

    /** Built-in BITMAP_AND_AGG with retraction aggregate function that returns bitmap. */
    public static final class BitmapAndWithRetractAggFunction
            extends AbstractBitmapAndWithRetractAggFunction<Bitmap> {

        public BitmapAndWithRetractAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BITMAP();
        }

        @Override
        public Bitmap getValue(BitmapAndWithRetractAccumulator acc) {
            if (acc.bitmapCount <= 0) {
                return null;
            }

            RoaringBitmapData bitmap = RoaringBitmapData.empty();
            try {
                for (Map.Entry<Integer, Integer> entry : acc.valueCount.entries()) {
                    if (entry.getValue() == acc.bitmapCount) {
                        bitmap.add(entry.getKey());
                    }
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(e);
            }

            return bitmap;
        }
    }

    /**
     * Built-in BITMAP_AND_CARDINALITY_AGG with retraction aggregate function that returns
     * cardinality.
     */
    public static final class BitmapAndCardinalityWithRetractAggFunction
            extends AbstractBitmapAndWithRetractAggFunction<Long> {

        public BitmapAndCardinalityWithRetractAggFunction(LogicalType valueType) {
            super(valueType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.BIGINT();
        }

        @Override
        public Long getValue(BitmapAndWithRetractAccumulator acc) {
            if (acc.bitmapCount <= 0) {
                return null;
            }

            long cardinality = 0L;
            try {
                for (Map.Entry<Integer, Integer> entry : acc.valueCount.entries()) {
                    if (entry.getValue() == acc.bitmapCount) {
                        cardinality++;
                    }
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(e);
            }

            return cardinality;
        }
    }
}
