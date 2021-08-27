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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class UnionWithTempOperator<T> implements Driver<Function, T> {

    private static final int CACHED_INPUT = 0;
    private static final int STREAMED_INPUT = 1;

    private TaskContext<Function, T> taskContext;

    private volatile boolean running;

    @Override
    public void setup(TaskContext<Function, T> context) {
        this.taskContext = context;
        this.running = true;
    }

    @Override
    public int getNumberOfInputs() {
        return 2;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 0;
    }

    @Override
    public Class<Function> getStubType() {
        return null; // no UDF
    }

    @Override
    public void prepare() {}

    @Override
    public void run() throws Exception {
        final Counter numRecordsIn =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        final Counter numRecordsOut =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

        final Collector<T> output =
                new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);
        T reuse =
                this.taskContext
                        .<T>getInputSerializer(STREAMED_INPUT)
                        .getSerializer()
                        .createInstance();
        T record;

        final MutableObjectIterator<T> input = this.taskContext.getInput(STREAMED_INPUT);
        while (this.running && ((record = input.next(reuse)) != null)) {
            numRecordsIn.inc();
            output.collect(record);
        }

        final MutableObjectIterator<T> cache = this.taskContext.getInput(CACHED_INPUT);
        while (this.running && ((record = cache.next(reuse)) != null)) {
            numRecordsIn.inc();
            output.collect(record);
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void cancel() {
        this.running = false;
    }
}
