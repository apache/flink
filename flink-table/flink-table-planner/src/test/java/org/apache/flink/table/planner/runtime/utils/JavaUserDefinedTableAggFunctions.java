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
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/** Test table aggregate table functions. */
public class JavaUserDefinedTableAggFunctions {

    /** Mutable accumulator of structured type for the table aggregate function. */
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
