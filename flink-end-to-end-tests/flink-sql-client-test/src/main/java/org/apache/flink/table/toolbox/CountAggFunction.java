/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.toolbox;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/** Count agg aggregate. */
public class CountAggFunction extends AggregateFunction<Long, CountAggFunction.CountAccumulator> {

    public void accumulate(CountAccumulator acc, Long value) {
        if (value != null) {
            acc.f0 += 1L;
        }
    }

    public void accumulate(CountAccumulator acc) {
        acc.f0 += 1L;
    }

    public void retract(CountAccumulator acc, Long value) {
        if (value != null) {
            acc.f0 -= 1L;
        }
    }

    public void retract(CountAccumulator acc) {
        acc.f0 -= 1L;
    }

    @Override
    public Long getValue(CountAccumulator acc) {
        return acc.f0;
    }

    public void merge(CountAccumulator acc, Iterable<CountAccumulator> its) {
        Iterator<CountAccumulator> iter = its.iterator();
        while (iter.hasNext()) {
            acc.f0 += iter.next().f0;
        }
    }

    @Override
    public CountAccumulator createAccumulator() {
        return new CountAccumulator();
    }

    /** The initial accumulator for count aggregate function. */
    public static class CountAccumulator extends Tuple1<Long> {

        public CountAccumulator() {
            this.f0 = 0L; // count
        }
    }
}
