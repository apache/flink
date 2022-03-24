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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/** Test aggregator functions. */
public class JavaUserDefinedAggFunctions {

    /** Accumulator of VarSumAggFunction. */
    public static class VarSumAcc {
        public long sum;
    }

    /** Only used for test. */
    public static class VarSumAggFunction extends AggregateFunction<Long, VarSumAcc> {

        @Override
        public VarSumAcc createAccumulator() {
            return new VarSumAcc();
        }

        public void accumulate(VarSumAcc acc, Integer... args) {
            for (Integer x : args) {
                if (x != null) {
                    acc.sum += x.longValue();
                }
            }
        }

        @Override
        public Long getValue(VarSumAcc accumulator) {
            return accumulator.sum;
        }
    }

    /**
     * Only used for test. The difference between the class and VarSumAggFunction is accumulator
     * type.
     */
    public static class VarSum1AggFunction extends AggregateFunction<Long, VarSumAcc> {

        @Override
        public VarSumAcc createAccumulator() {
            return new VarSumAcc();
        }

        public void accumulate(VarSumAcc acc, Integer... args) {
            for (Integer x : args) {
                if (x != null) {
                    acc.sum += x.longValue();
                }
            }
        }

        @Override
        public Long getValue(VarSumAcc accumulator) {
            return accumulator.sum;
        }

        @Override
        public TypeInformation<Long> getResultType() {
            return Types.LONG;
        }
    }

    /**
     * Only used for test. The difference between the class and VarSumAggFunction is accumulator
     * type.
     */
    public static class VarSum2AggFunction extends AggregateFunction<Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        public void accumulate(Long acc, Integer... args) {
            for (Integer x : args) {
                if (x != null) {
                    acc += x.longValue();
                }
            }
        }

        @Override
        public Long getValue(Long accumulator) {
            return accumulator;
        }

        @Override
        public TypeInformation<Long> getResultType() {
            return Types.LONG;
        }
    }

    /** Accumulator for WeightedAvg. */
    public static class WeightedAvgAccum {
        public long sum = 0;
        public int count = 0;
    }

    /** Base class for WeightedAvg. */
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {
        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Long getValue(WeightedAvgAccum accumulator) {
            if (accumulator.count == 0) {
                return null;
            } else {
                return accumulator.sum / accumulator.count;
            }
        }

        // overloaded accumulate method
        // dummy to test constants
        public void accumulate(
                WeightedAvgAccum accumulator,
                Long iValue,
                Integer iWeight,
                Integer x,
                String string) {
            accumulator.sum += (iValue + Integer.parseInt(string)) * iWeight;
            accumulator.count += iWeight;
        }

        // overloaded accumulate method
        public void accumulate(WeightedAvgAccum accumulator, Long iValue, Long iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }

        // Overloaded accumulate method
        public void accumulate(WeightedAvgAccum accumulator, Integer iValue, Integer iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }

    /** A WeightedAvg class with merge method. */
    public static class WeightedAvgWithMerge extends WeightedAvg {
        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

        @Override
        public String toString() {
            return "myWeightedAvg";
        }
    }

    /** A WeightedAvg class with merge and reset method. */
    public static class WeightedAvgWithMergeAndReset extends WeightedAvgWithMerge {
        private static final long serialVersionUID = -2721882038448388054L;

        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0L;
        }
    }

    /** A WeightedAvg class with retract method. */
    public static class WeightedAvgWithRetract extends WeightedAvg {
        // Overloaded retract method
        public void retract(WeightedAvgAccum accumulator, long iValue, int iWeight) {
            accumulator.sum -= iValue * iWeight;
            accumulator.count -= iWeight;
        }

        // Overloaded retract method
        public void retract(WeightedAvgAccum accumulator, int iValue, int iWeight) {
            accumulator.sum -= iValue * iWeight;
            accumulator.count -= iWeight;
        }
    }

    /** Accumulator of ConcatDistinctAgg. */
    public static class ConcatAcc {
        public MapView<String, Boolean> map = new MapView<>(Types.STRING, Types.BOOLEAN);
        public ListView<String> list = new ListView<>(Types.STRING);
    }

    /** Concat distinct aggregate. */
    public static class ConcatDistinctAggFunction extends AggregateFunction<String, ConcatAcc> {

        private static final long serialVersionUID = -2678065132752935739L;
        private static final String DELIMITER = "|";

        public void accumulate(ConcatAcc acc, String value) throws Exception {
            if (value != null) {
                if (!acc.map.contains(value)) {
                    acc.map.put(value, true);
                    acc.list.add(value);
                }
            }
        }

        public void merge(ConcatAcc acc, Iterable<ConcatAcc> its) throws Exception {
            for (ConcatAcc otherAcc : its) {
                Iterable<String> accList = otherAcc.list.get();
                if (accList != null) {
                    for (String value : accList) {
                        if (!acc.map.contains(value)) {
                            acc.map.put(value, true);
                            acc.list.add(value);
                        }
                    }
                }
            }
        }

        @Override
        public ConcatAcc createAccumulator() {
            return new ConcatAcc();
        }

        @Override
        public String getValue(ConcatAcc acc) {
            try {
                Iterable<String> accList = acc.list.get();
                if (accList == null || !accList.iterator().hasNext()) {
                    return null;
                } else {
                    StringBuilder builder = new StringBuilder();
                    boolean isFirst = true;
                    for (String value : accList) {
                        if (!isFirst) {
                            builder.append(DELIMITER);
                        }
                        builder.append(value);
                        isFirst = false;
                    }
                    return builder.toString();
                }
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    /** CountDistinct accumulator. */
    public static class CountDistinctAccum {
        public MapView<String, Integer> map;
        public long count;
    }

    /** CountDistinct aggregate. */
    public static class CountDistinct extends AggregateFunction<Long, CountDistinctAccum> {

        private static final long serialVersionUID = -8369074832279506466L;

        @Override
        public CountDistinctAccum createAccumulator() {
            CountDistinctAccum accum = new CountDistinctAccum();
            accum.map =
                    new MapView<>(
                            org.apache.flink.table.api.Types.STRING(),
                            org.apache.flink.table.api.Types.INT());
            accum.count = 0L;
            return accum;
        }

        // Overloaded accumulate method
        public void accumulate(CountDistinctAccum accumulator, String id) {
            try {
                Integer cnt = accumulator.map.get(id);
                if (cnt != null) {
                    cnt += 1;
                    accumulator.map.put(id, cnt);
                } else {
                    accumulator.map.put(id, 1);
                    accumulator.count += 1;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Overloaded accumulate method
        public void accumulate(CountDistinctAccum accumulator, Long id) {
            try {
                Integer cnt = accumulator.map.get(String.valueOf(id));
                if (cnt != null) {
                    cnt += 1;
                    accumulator.map.put(String.valueOf(id), cnt);
                } else {
                    accumulator.map.put(String.valueOf(id), 1);
                    accumulator.count += 1;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public Long getValue(CountDistinctAccum accumulator) {
            return accumulator.count;
        }
    }

    /** CountDistinct aggregate with merge. */
    public static class CountDistinctWithMerge extends CountDistinct {

        private static final long serialVersionUID = -9028804545597563968L;

        // Overloaded merge method
        public void merge(CountDistinctAccum acc, Iterable<CountDistinctAccum> it) {
            for (CountDistinctAccum mergeAcc : it) {
                try {
                    Iterable<String> keys = mergeAcc.map.keys();
                    if (keys != null) {
                        for (String key : keys) {
                            Integer cnt = mergeAcc.map.get(key);
                            if (acc.map.contains(key)) {
                                acc.map.put(key, acc.map.get(key) + cnt);
                            } else {
                                acc.map.put(key, cnt);
                                acc.count += 1;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /** CountDistinct aggregate with merge and reset. */
    public static class CountDistinctWithMergeAndReset extends CountDistinctWithMerge {

        // Overloaded retract method
        public void resetAccumulator(CountDistinctAccum acc) {
            acc.map.clear();
            acc.count = 0;
        }
    }

    /** CountDistinct aggregate with retract. */
    public static class CountDistinctWithRetractAndReset extends CountDistinct {

        // Overloaded retract method
        public void retract(CountDistinctAccum accumulator, Long id) {
            try {
                Integer cnt = accumulator.map.get(String.valueOf(id));
                if (cnt != null) {
                    cnt -= 1;
                    if (cnt <= 0) {
                        accumulator.map.remove(String.valueOf(id));
                        accumulator.count -= 1;
                    } else {
                        accumulator.map.put(String.valueOf(id), cnt);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Overloaded retract method
        public void resetAccumulator(CountDistinctAccum acc) {
            acc.map.clear();
            acc.count = 0;
        }
    }

    /** Counts how often the first argument was larger than the second argument. */
    public static class LargerThanCount extends AggregateFunction<Long, Tuple1<Long>> {

        public void accumulate(Tuple1<Long> acc, Long a, Long b) {
            if (a > b) {
                acc.f0 += 1;
            }
        }

        public void retract(Tuple1<Long> acc, Long a, Long b) {
            if (a > b) {
                acc.f0 -= 1;
            }
        }

        @Override
        public Long getValue(Tuple1<Long> accumulator) {
            return accumulator.f0;
        }

        @Override
        public Tuple1<Long> createAccumulator() {
            return Tuple1.of(0L);
        }
    }
}
