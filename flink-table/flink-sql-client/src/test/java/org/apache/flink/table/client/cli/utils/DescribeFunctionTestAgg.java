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

package org.apache.flink.table.client.cli.utils;

import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Minimal {@link AggregateFunction} used by {@code function.q} to exercise {@code DESCRIBE FUNCTION
 * EXTENDED} on a function whose accumulator surfaces as a {@code state:} row. The body is a no-op
 * since only the introspection output is asserted.
 */
public class DescribeFunctionTestAgg extends AggregateFunction<Long, DescribeFunctionTestAgg.Acc> {

    /** Accumulator type for the aggregate. */
    public static class Acc {
        public Long sum;
        public Long count;
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    public void accumulate(@StateHint(ttl = "2 d") Acc acc, Long value) {}

    @Override
    public Long getValue(Acc accumulator) {
        return 0L;
    }
}
