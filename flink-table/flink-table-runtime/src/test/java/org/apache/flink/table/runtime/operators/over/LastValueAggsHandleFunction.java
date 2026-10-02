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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;

/**
 * Test {@link AggsHandleFunction} whose accumulator shape, (lastValue, count), differs from its
 * result shape, (lastValue) only - mirroring built-in aggregates like LAST_VALUE with retraction,
 * where the accumulator carries bookkeeping fields beyond what getValue() returns.
 */
public class LastValueAggsHandleFunction implements AggsHandleFunction {

    private final int inputIndex;
    private Long lastValue;
    private long count;

    public LastValueAggsHandleFunction(int inputIndex) {
        this.inputIndex = inputIndex;
    }

    @Override
    public void open(StateDataViewStore store) throws Exception {}

    @Override
    public void accumulate(RowData input) throws Exception {
        lastValue = input.getLong(inputIndex);
        count++;
    }

    @Override
    public void retract(RowData input) throws Exception {
        count--;
    }

    @Override
    public void merge(RowData accumulator) throws Exception {
        lastValue = accumulator.getLong(0);
        count += accumulator.getLong(1);
    }

    @Override
    public void setAccumulators(RowData accumulator) throws Exception {
        lastValue = accumulator.getLong(0);
        count = accumulator.getLong(1);
    }

    @Override
    public void resetAccumulators() throws Exception {
        lastValue = null;
        count = 0;
    }

    @Override
    public RowData getAccumulators() throws Exception {
        return GenericRowData.of(lastValue, count);
    }

    @Override
    public RowData createAccumulators() throws Exception {
        return GenericRowData.of(0L, 0L);
    }

    @Override
    public RowData getValue() throws Exception {
        return GenericRowData.of(lastValue);
    }

    @Override
    public void setWindowSize(int windowSize) {}

    @Override
    public void cleanup() throws Exception {}

    @Override
    public void close() throws Exception {}
}
