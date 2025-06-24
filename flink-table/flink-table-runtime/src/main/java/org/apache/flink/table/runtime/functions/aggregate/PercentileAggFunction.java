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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.math3.util.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Built-in PERCENTILE aggregate function. */
@Internal
public abstract class PercentileAggFunction<T>
        extends BuiltInAggregateFunction<T, PercentileAggFunction.PercentileAccumulator> {

    protected final transient DataType valueType;
    protected final transient DataType frequencyType;

    public PercentileAggFunction(LogicalType inputType, LogicalType frequencyType) {
        this.valueType = toInternalDataType(inputType);
        this.frequencyType = frequencyType == null ? null : toInternalDataType(frequencyType);
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    /** Accumulator for PERCENTILE. */
    public static class PercentileAccumulator {

        public double[] percentages;
        public MapView<Double, Long> valueCount;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PercentileAccumulator that = (PercentileAccumulator) o;
            return Arrays.equals(percentages, that.percentages)
                    && Objects.equals(valueCount, that.valueCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(percentages), valueCount.hashCode());
        }

        public Double[] getValue() {
            // calculate totalCount here to avoid the inconsistent expiration of the two different
            // states (ValueState acc.totalCount vs MapState acc.valueCount) when operating them
            // serially
            long totalCount = 0L;

            // get a sorted list from hashmap for single traversal
            List<Map.Entry<Double, Long>> sortedList = new ArrayList<>();
            try {
                for (Map.Entry<Double, Long> entry : valueCount.entries()) {
                    sortedList.add(entry);
                    totalCount += entry.getValue();
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException(e);
            }

            if (totalCount <= 0) {
                return null;
            }

            sortedList.sort(Map.Entry.comparingByKey());

            // calculate the position for each percentage and sort it, so that all percentiles can
            // be obtained with a single traversal
            List<Pair<Double, Integer>> sortedPercentages = new ArrayList<>();
            for (int index = 0; index < percentages.length; index++) {
                sortedPercentages.add(new Pair<>(percentages[index] * (totalCount - 1) + 1, index));
            }
            sortedPercentages.sort(Comparator.comparing(Pair::getKey));

            Double[] percentiles = new Double[percentages.length];

            long preCnt = sortedList.get(0).getValue();
            for (int i = 0, j = 0; i < sortedPercentages.size(); i++) {
                Pair<Double, Integer> entry = sortedPercentages.get(i);
                double position = entry.getKey();
                long lower = (long) Math.floor(position);
                long higher = (long) Math.ceil(position);

                // lower <= (totalCount - 1) * percentage + 1 <= totalCount
                // hence, j will never overflow
                while (preCnt < lower) {
                    j++;
                    preCnt += sortedList.get(j).getValue();
                }

                percentiles[entry.getValue()] =
                        preCnt >= higher
                                // 1. position is between two same values
                                // 2. position corresponds to an exact index in the list
                                ? sortedList.get(j).getKey()
                                // linear interpolation to get the exact percentile
                                : (higher - position) * sortedList.get(j).getKey()
                                        + (position - lower) * sortedList.get(j + 1).getKey();
            }

            return percentiles;
        }

        public void setPercentages(Double percentage) {
            // save and verify percentage values only the first time to avoid redundant assignments
            // and verifications
            if (percentage < 0.0 || percentage > 1.0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Percentage of PERCENTILE should be between [0.0, 1.0], but was '%s'.",
                                percentage));
            }
            percentages = new double[] {percentage};
        }

        public void setPercentages(Double[] percentage) {
            // save and verify percentage values only the first time to avoid redundant assignments
            // and verifications
            percentages = new double[percentage.length];
            for (int i = 0; i < percentages.length; i++) {
                if (percentage[i] < 0.0 || percentage[i] > 1.0) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Percentage of PERCENTILE should be between [0.0, 1.0], but was '%s'.",
                                    percentage[i]));
                }
                percentages[i] = percentage[i];
            }
        }
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                PercentileAccumulator.class,
                DataTypes.FIELD(
                        "percentages",
                        DataTypes.ARRAY(DataTypes.DOUBLE()).bridgedTo(double[].class)),
                DataTypes.FIELD(
                        "valueCount",
                        MapView.newMapViewDataType(DataTypes.DOUBLE(), DataTypes.BIGINT())));
    }

    @Override
    public PercentileAccumulator createAccumulator() {
        final PercentileAccumulator acc = new PercentileAccumulator();
        acc.percentages = null;
        acc.valueCount = new MapView<>();
        return acc;
    }

    // --------------------------------------------------------------------------------------------
    // accumulate methods
    // --------------------------------------------------------------------------------------------

    public void accumulate(PercentileAccumulator acc, @Nullable Object value, Double percentage)
            throws Exception {
        if (acc.percentages == null) {
            acc.setPercentages(percentage);
        }
        update(acc, value, 1L);
    }

    public void accumulate(
            PercentileAccumulator acc,
            @Nullable Object value,
            Double percentage,
            @Nullable Number frequency)
            throws Exception {
        if (acc.percentages == null) {
            acc.setPercentages(percentage);
        }
        if (frequency == null || frequency.longValue() <= 0) {
            return;
        }
        update(acc, value, frequency.longValue());
    }

    public void accumulate(PercentileAccumulator acc, @Nullable Object value, Double[] percentage)
            throws Exception {
        if (acc.percentages == null) {
            acc.setPercentages(percentage);
        }
        update(acc, value, 1L);
    }

    public void accumulate(
            PercentileAccumulator acc,
            @Nullable Object value,
            Double[] percentage,
            @Nullable Number frequency)
            throws Exception {
        if (acc.percentages == null) {
            acc.setPercentages(percentage);
        }
        if (frequency == null || frequency.longValue() <= 0) {
            return;
        }
        update(acc, value, frequency.longValue());
    }

    private void update(PercentileAccumulator acc, @Nullable Object value, long frequency)
            throws Exception {
        // skip input if value is null
        if (value == null) {
            return;
        }

        double val =
                (value instanceof Number)
                        ? ((Number) value).doubleValue()
                        : DecimalDataUtils.doubleValue((DecimalData) value);

        long cnt = Optional.ofNullable(acc.valueCount.get(val)).orElse(0L) + frequency;
        if (cnt != 0) {
            acc.valueCount.put(val, cnt);
        } else {
            acc.valueCount.remove(val);
        }
    }

    // --------------------------------------------------------------------------------------------
    // retract methods
    // --------------------------------------------------------------------------------------------

    public void retract(PercentileAccumulator acc, @Nullable Object value, Double percentage)
            throws Exception {
        update(acc, value, -1L);
    }

    public void retract(
            PercentileAccumulator acc,
            @Nullable Object value,
            Double percentage,
            @Nullable Number frequency)
            throws Exception {
        if (frequency == null || frequency.longValue() <= 0) {
            return;
        }
        update(acc, value, -frequency.longValue());
    }

    public void retract(PercentileAccumulator acc, @Nullable Object value, Double[] percentage)
            throws Exception {
        update(acc, value, -1L);
    }

    public void retract(
            PercentileAccumulator acc,
            @Nullable Object value,
            Double[] percentage,
            @Nullable Number frequency)
            throws Exception {
        if (frequency == null || frequency.longValue() <= 0) {
            return;
        }
        update(acc, value, -frequency.longValue());
    }

    // --------------------------------------------------------------------------------------------
    // merge methods
    // --------------------------------------------------------------------------------------------

    public void merge(PercentileAccumulator acc, Iterable<PercentileAccumulator> its)
            throws Exception {
        for (PercentileAccumulator mergedAcc : its) {
            if (acc.percentages == null && mergedAcc.percentages != null) {
                acc.percentages = mergedAcc.percentages.clone();
            }

            for (Map.Entry<Double, Long> entry : mergedAcc.valueCount.entries()) {
                long cnt =
                        Optional.ofNullable(acc.valueCount.get(entry.getKey())).orElse(0L)
                                + entry.getValue();
                if (cnt != 0) {
                    acc.valueCount.put(entry.getKey(), cnt);
                } else {
                    acc.valueCount.remove(entry.getKey());
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // implementation classes
    // --------------------------------------------------------------------------------------------

    /** Percentile agg function with only one percentage parameter. */
    public static class SinglePercentileAggFunction extends PercentileAggFunction<Double> {

        public SinglePercentileAggFunction(LogicalType valueType, LogicalType frequencyType) {
            super(valueType, frequencyType);
        }

        @Override
        public List<DataType> getArgumentDataTypes() {
            return frequencyType == null
                    ? Arrays.asList(valueType, DataTypes.DOUBLE().notNull())
                    : Arrays.asList(valueType, DataTypes.DOUBLE().notNull(), frequencyType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.DOUBLE();
        }

        @Override
        public Double getValue(PercentileAccumulator acc) {
            if (acc.percentages == null) {
                return null;
            }
            Double[] result = acc.getValue();
            return result == null ? null : result[0];
        }
    }

    /** Percentile agg function with multiple percentage parameters. */
    public static class MultiPercentileAggFunction extends PercentileAggFunction<Double[]> {

        public MultiPercentileAggFunction(LogicalType valueType, LogicalType frequencyType) {
            super(valueType, frequencyType);
        }

        @Override
        public List<DataType> getArgumentDataTypes() {
            return frequencyType == null
                    ? Arrays.asList(
                            valueType, DataTypes.ARRAY(DataTypes.DOUBLE().notNull()).notNull())
                    : Arrays.asList(
                            valueType,
                            DataTypes.ARRAY(DataTypes.DOUBLE().notNull()).notNull(),
                            frequencyType);
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.ARRAY(DataTypes.DOUBLE());
        }

        @Override
        public Double[] getValue(PercentileAccumulator acc) {
            if (acc.percentages == null || acc.percentages.length == 0) {
                return null;
            }
            return acc.getValue();
        }
    }
}
