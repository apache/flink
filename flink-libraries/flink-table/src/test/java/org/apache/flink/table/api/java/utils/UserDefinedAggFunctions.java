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
package org.apache.flink.table.api.java.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

public class UserDefinedAggFunctions {
    // Accumulator for test requiresOver
    public static class Accumulator0 extends Tuple2<Long, Integer>{}

    // Test for requiresOver
    public static class OverAgg0 extends AggregateFunction<Long, Accumulator0> {
        @Override
        public Accumulator0 createAccumulator() {
            return new Accumulator0();
        }

        @Override
        public Long getValue(Accumulator0 accumulator) {
            return 1L;
        }

        //Overloaded accumulate method
        public void accumulate(Accumulator0 accumulator, long iValue, int iWeight) {
        }

        @Override
        public boolean requiresOver() {
            return true;
        }
    }

    // Accumulator for WeightedAvg
    public static class WeightedAvgAccum extends Tuple2<Long, Integer> {
        public long sum = 0;
        public int count = 0;
    }

    // Base class for WeightedAvg
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {
        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Long getValue(WeightedAvgAccum accumulator) {
            if (accumulator.count == 0)
                return null;
            else
                return accumulator.sum/accumulator.count;
        }

        //Overloaded accumulate method
        public void accumulate(WeightedAvgAccum accumulator, long iValue, int iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }

        //Overloaded accumulate method
        public void accumulate(WeightedAvgAccum accumulator, int iValue, int iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }

    // A WeightedAvg class with merge method
    public static class WeightedAvgWithMerge extends WeightedAvg {
        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }
    }

    // A WeightedAvg class with merge and reset method
    public static class WeightedAvgWithMergeAndReset extends WeightedAvgWithMerge {
        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0L;
        }
    }

    // A WeightedAvg class with retract method
    public static class WeightedAvgWithRetract extends WeightedAvg {
        //Overloaded retract method
        public void retract(WeightedAvgAccum accumulator, long iValue, int iWeight) {
            accumulator.sum -= iValue * iWeight;
            accumulator.count -= iWeight;
        }

        //Overloaded retract method
        public void retract(WeightedAvgAccum accumulator, int iValue, int iWeight) {
            accumulator.sum -= iValue * iWeight;
            accumulator.count -= iWeight;
        }
    }
}
