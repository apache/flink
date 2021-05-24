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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * Map task which is executed by a Task Manager. The task has a single input and one or multiple
 * outputs. It is provided with a MapFunction implementation.
 *
 * <p>The MapTask creates an iterator over all key-value pairs of its input and hands that to the
 * <code>map()</code> method of the MapFunction.
 *
 * @see org.apache.flink.api.common.functions.MapFunction
 * @param <IT> The mapper's input data type.
 * @param <OT> The mapper's output data type.
 */
public class MapDriver<IT, OT> implements Driver<MapFunction<IT, OT>, OT> {

    private TaskContext<MapFunction<IT, OT>, OT> taskContext;

    private volatile boolean running;

    private boolean objectReuseEnabled = false;

    @Override
    public void setup(TaskContext<MapFunction<IT, OT>, OT> context) {
        this.taskContext = context;
        this.running = true;

        ExecutionConfig executionConfig = taskContext.getExecutionConfig();
        this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();
    }

    @Override
    public int getNumberOfInputs() {
        return 1;
    }

    @Override
    public Class<MapFunction<IT, OT>> getStubType() {
        @SuppressWarnings("unchecked")
        final Class<MapFunction<IT, OT>> clazz =
                (Class<MapFunction<IT, OT>>) (Class<?>) MapFunction.class;
        return clazz;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 0;
    }

    @Override
    public void prepare() {
        // nothing, since a mapper does not need any preparation
    }

    @Override
    public void run() throws Exception {
        final Counter numRecordsIn =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        final Counter numRecordsOut =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        // cache references on the stack
        final MutableObjectIterator<IT> input = this.taskContext.getInput(0);
        final MapFunction<IT, OT> function = this.taskContext.getStub();
        final Collector<OT> output =
                new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

        if (objectReuseEnabled) {
            IT record = this.taskContext.<IT>getInputSerializer(0).getSerializer().createInstance();

            while (this.running && ((record = input.next(record)) != null)) {
                numRecordsIn.inc();
                output.collect(function.map(record));
            }
        } else {
            IT record = null;

            while (this.running && ((record = input.next()) != null)) {
                numRecordsIn.inc();
                output.collect(function.map(record));
            }
        }
    }

    @Override
    public void cleanup() {
        // mappers need no cleanup, since no strategies are used.
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
