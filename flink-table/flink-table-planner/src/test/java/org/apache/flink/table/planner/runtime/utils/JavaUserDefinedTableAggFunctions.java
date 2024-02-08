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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Function for testing basic functionality of TableAggregateFunction.
 *
 * <p>Note: Functions in this class suffer performance problem. Only use it in tests.
 */
public class JavaUserDefinedTableAggFunctions {

    /** Accumulator for {@link Top3}. */
    public static class Top3Accumulator {
        public Map<Integer, Integer> data;
        public Integer size;
        public Integer smallest;
    }

    /**
     * Function that takes (value INT), stores intermediate results in a structured type of {@link
     * Top3Accumulator}, and returns the result as a structured type of {@link Tuple2} for value and
     * rank.
     */
    public static class Top3
            extends TableAggregateFunction<Tuple2<Integer, Integer>, Top3Accumulator> {

        @Override
        public Top3Accumulator createAccumulator() {
            Top3Accumulator acc = new Top3Accumulator();
            acc.data = new HashMap<>();
            acc.size = 0;
            acc.smallest = Integer.MAX_VALUE;
            return acc;
        }

        public void add(Top3Accumulator acc, Integer v) {
            Integer cnt = acc.data.get(v);
            acc.size += 1;
            if (cnt == null) {
                cnt = 0;
            }
            acc.data.put(v, cnt + 1);
        }

        public void delete(Top3Accumulator acc, Integer v) {
            if (acc.data.containsKey(v)) {
                acc.size -= 1;
                int cnt = acc.data.get(v) - 1;
                if (cnt == 0) {
                    acc.data.remove(v);
                } else {
                    acc.data.put(v, cnt);
                }
            }
        }

        public static void updateSmallest(Top3Accumulator acc) {
            acc.smallest = Integer.MAX_VALUE;
            for (Integer key : acc.data.keySet()) {
                if (key < acc.smallest) {
                    acc.smallest = key;
                }
            }
        }

        public void accumulate(Top3Accumulator acc, Integer v) {
            if (acc.size == 0) {
                acc.size = 1;
                acc.smallest = v;
                acc.data.put(v, 1);
            } else if (acc.size < 3) {
                add(acc, v);
                if (v < acc.smallest) {
                    acc.smallest = v;
                }
            } else if (v > acc.smallest) {
                delete(acc, acc.smallest);
                add(acc, v);
                updateSmallest(acc);
            }
        }

        public void merge(Top3Accumulator acc, Iterable<Top3Accumulator> its) {
            for (Top3Accumulator otherAcc : its) {
                otherAcc.data.forEach(
                        (key, value) -> {
                            for (int i = 0; i < value; i++) {
                                accumulate(acc, key);
                            }
                        });
            }
        }

        public void emitValue(Top3Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {
            List<Map.Entry<Integer, Integer>> entries = new ArrayList<>(acc.data.entrySet());
            entries.sort(Map.Entry.<Integer, Integer>comparingByKey().reversed());

            int rank = 1;
            for (Map.Entry<Integer, Integer> entry : entries) {
                int count = entry.getValue();
                while (count-- > 0) {
                    // output the value and the rank number
                    out.collect(new Tuple2<>(entry.getKey(), rank++));
                }
            }
        }
    }

    /** Accumulator for {@link Top3WithMapView}. */
    public static class Top3WithMapViewAccumulator {
        public MapView<Integer, Integer> data;
        public Integer size;
        public Integer smallest;
    }

    /**
     * Function that takes (value INT), stores intermediate results in a structured type of {@link
     * Top3Accumulator}, and returns the result as a structured type of {@link Tuple2} for value and
     * rank.
     *
     * <p>Difference from {@link Top3} is that it is a function for testing {@link MapView}.
     */
    public static class Top3WithMapView
            extends TableAggregateFunction<Tuple2<Integer, Integer>, Top3WithMapViewAccumulator> {

        @Override
        public Top3WithMapViewAccumulator createAccumulator() {
            Top3WithMapViewAccumulator acc = new Top3WithMapViewAccumulator();
            acc.data = new MapView<>();
            acc.size = 0;
            acc.smallest = Integer.MAX_VALUE;
            return acc;
        }

        public void add(Top3WithMapViewAccumulator acc, int v) throws Exception {
            Integer cnt = acc.data.get(v);
            acc.size += 1;
            if (cnt == null) {
                cnt = 0;
            }
            acc.data.put(v, cnt + 1);
        }

        public void delete(Top3WithMapViewAccumulator acc, int v) throws Exception {
            if (acc.data.contains(v)) {
                acc.size -= 1;
                int cnt = acc.data.get(v) - 1;
                if (cnt == 0) {
                    acc.data.remove(v);
                } else {
                    acc.data.put(v, cnt);
                }
            }
        }

        public void updateSmallest(Top3WithMapViewAccumulator acc) throws Exception {
            acc.smallest = Integer.MAX_VALUE;
            acc.data
                    .keys()
                    .forEach(
                            key -> {
                                if (key < acc.smallest) {
                                    acc.smallest = key;
                                }
                            });
        }

        public void accumulate(Top3WithMapViewAccumulator acc, int v) throws Exception {
            if (acc.size == 0) {
                acc.size = 1;
                acc.smallest = v;
                acc.data.put(v, 1);
            } else if (acc.size < 3) {
                add(acc, v);
                if (v < acc.smallest) {
                    acc.smallest = v;
                }
            } else if (v > acc.smallest) {
                delete(acc, acc.smallest);
                add(acc, v);
                updateSmallest(acc);
            }
        }

        public void emitValue(
                Top3WithMapViewAccumulator acc, Collector<Tuple2<Integer, Integer>> out)
                throws Exception {
            Iterator<Map.Entry<Integer, Integer>> iterator = acc.data.iterator();
            List<Integer> allKeys = new ArrayList<>();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Integer> pair = iterator.next();
                for (int i = 0; i < pair.getValue(); i++) {
                    allKeys.add(pair.getKey());
                }
            }

            allKeys.sort(Comparator.reverseOrder());

            for (int index = 0; index < allKeys.size(); index++) {
                // output the value and the rank number
                out.collect(new Tuple2<>(allKeys.get(index), index + 1));
            }
        }
    }

    /** Accumulator for {@link Top3WithRetractInput}. */
    public static class Top3WithRetractInputAccumulator {
        @DataTypeHint("RAW")
        public List<Integer> data;
    }

    /**
     * Function that takes (value INT), stores intermediate results in a structured type of {@link
     * Top3Accumulator}, and returns the result as a structured type of {@link Tuple2} for value and
     * rank.
     *
     * <p>Difference from {@link Top3} is that it is a function for testing retract input.
     */
    public static class Top3WithRetractInput
            extends TableAggregateFunction<
                    Tuple2<Integer, Integer>, Top3WithRetractInputAccumulator> {

        @Override
        public Top3WithRetractInputAccumulator createAccumulator() {
            Top3WithRetractInputAccumulator acc = new Top3WithRetractInputAccumulator();
            acc.data = new ArrayList<>();
            return acc;
        }

        public void accumulate(Top3WithRetractInputAccumulator acc, Integer v) {
            acc.data.add(v);
        }

        public void retract(Top3WithRetractInputAccumulator acc, Integer v) {
            acc.data.remove(v);
        }

        public void emitValue(
                Top3WithRetractInputAccumulator acc, Collector<Tuple2<Integer, Integer>> out) {
            acc.data.sort(Comparator.reverseOrder());
            int i = 0;
            for (Integer v : acc.data) {
                if (i >= 3) {
                    break;
                }
                out.collect(new Tuple2<>(v, ++i));
            }
        }
    }

    /** Function for testing internal accumulator type. */
    @FunctionHint(accumulator = @DataTypeHint(value = "ROW<i INT>", bridgedTo = RowData.class))
    public static class TableAggSum extends TableAggregateFunction<Integer, RowData> {

        @Override
        public RowData createAccumulator() {
            GenericRowData acc = new GenericRowData(1);
            acc.setField(0, 0);
            return acc;
        }

        public void accumulate(RowData rowData, Integer v) {
            GenericRowData acc = (GenericRowData) rowData;
            acc.setField(0, acc.getInt(0) + v);
        }

        public void emitValue(RowData rowData, Collector<Integer> out) {
            GenericRowData acc = (GenericRowData) rowData;
            // output two records
            int result = acc.getInt(0);
            out.collect(result);
            out.collect(result);
        }
    }

    /** Test function for plan test with result type {@link Tuple2}. */
    @SuppressWarnings("unused")
    public static class EmptyTableAggFunc
            extends TableAggregateFunction<Tuple2<Integer, Integer>, Top3Accumulator> {
        @Override
        public Top3Accumulator createAccumulator() {
            return new Top3Accumulator();
        }

        public void accumulate(Top3Accumulator acc, Timestamp category, Timestamp value) {}

        public void accumulate(Top3Accumulator acc, Long category, Timestamp value) {}

        public void accumulate(Top3Accumulator acc, Long category, Integer value) {}

        public void accumulate(Top3Accumulator acc, Integer value) {}

        public void emitValue(Top3Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {}
    }

    /** Test function for plan test with result type {@link Integer}. */
    @SuppressWarnings("unused")
    public static class EmptyTableAggFuncWithIntResultType
            extends TableAggregateFunction<Integer, Top3Accumulator> {

        @Override
        public Top3Accumulator createAccumulator() {
            return new Top3Accumulator();
        }

        public void accumulate(Top3Accumulator acc, Integer value) {}

        public void emitValue(Top3Accumulator acc, Collector<Integer> out) {}
    }

    /** Test function for plan test using python. */
    public static class PythonEmptyTableAggFunc
            extends TableAggregateFunction<Tuple2<Integer, Integer>, Top3Accumulator>
            implements PythonFunction {

        @Override
        public byte[] getSerializedPythonFunction() {
            return new byte[] {0};
        }

        @Override
        public PythonEnv getPythonEnv() {
            return null;
        }

        @Override
        public Top3Accumulator createAccumulator() {
            return new Top3Accumulator();
        }

        public void accumulate(Top3Accumulator acc, Integer value1, Integer value2) {}

        public void emitValue(Top3Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {}
    }

    /** Accumulator for {@link Top2}. */
    public static class Top2Accumulator {

        public Integer first;
        public Integer second;
        public Integer previousFirst;
        public Integer previousSecond;
    }

    /**
     * Function that takes (value INT), stores intermediate results in a structured type of {@link
     * Top2Accumulator}, and returns the result as a structured type of {@link Tuple2} for value and
     * rank.
     */
    public static class Top2
            extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        public void accumulate(Top2Accumulator acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
            for (Top2Accumulator otherAcc : it) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }

    /** Subclass of {@link Top2} to support emit incremental changes. */
    public static class IncrementalTop2 extends Top2 {
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = super.createAccumulator();
            acc.previousFirst = Integer.MIN_VALUE;
            acc.previousSecond = Integer.MIN_VALUE;
            return acc;
        }

        @Override
        public void accumulate(Top2Accumulator acc, Integer value) {
            acc.previousFirst = acc.first;
            acc.previousSecond = acc.second;
            super.accumulate(acc, value);
        }

        public void emitUpdateWithRetract(
                Top2Accumulator acc, RetractableCollector<Tuple2<Integer, Integer>> out) {
            // emit the value and rank only if they're changed
            if (!acc.first.equals(acc.previousFirst)) {
                if (!acc.previousFirst.equals(Integer.MIN_VALUE)) {
                    out.retract(Tuple2.of(acc.previousFirst, 1));
                }
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (!acc.second.equals(acc.previousSecond)) {
                if (!acc.previousSecond.equals(Integer.MIN_VALUE)) {
                    out.retract(Tuple2.of(acc.previousSecond, 2));
                }
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }
}
