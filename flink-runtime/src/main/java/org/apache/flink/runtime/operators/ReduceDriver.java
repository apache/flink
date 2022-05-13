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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reduce driver which is executed by a Task Manager. The task has a single input and one or
 * multiple outputs. It is provided with a ReduceFunction implementation.
 *
 * <p>The ReduceDriver creates an iterator over all records from its input. The iterator returns all
 * records grouped by their key. The elements are handed pairwise to the <code>reduce()</code>
 * method of the ReduceFunction.
 *
 * @see org.apache.flink.api.common.functions.ReduceFunction
 */
public class ReduceDriver<T> implements Driver<ReduceFunction<T>, T> {

    private static final Logger LOG = LoggerFactory.getLogger(ReduceDriver.class);

    private TaskContext<ReduceFunction<T>, T> taskContext;

    private MutableObjectIterator<T> input;

    private TypeSerializer<T> serializer;

    private TypeComparator<T> comparator;

    private volatile boolean running;

    private boolean objectReuseEnabled = false;

    // ------------------------------------------------------------------------

    @Override
    public void setup(TaskContext<ReduceFunction<T>, T> context) {
        this.taskContext = context;
        this.running = true;
    }

    @Override
    public int getNumberOfInputs() {
        return 1;
    }

    @Override
    public Class<ReduceFunction<T>> getStubType() {
        @SuppressWarnings("unchecked")
        final Class<ReduceFunction<T>> clazz =
                (Class<ReduceFunction<T>>) (Class<?>) ReduceFunction.class;
        return clazz;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 1;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void prepare() throws Exception {
        TaskConfig config = this.taskContext.getTaskConfig();
        if (config.getDriverStrategy() != DriverStrategy.SORTED_REDUCE) {
            throw new Exception(
                    "Unrecognized driver strategy for Reduce driver: "
                            + config.getDriverStrategy().name());
        }
        this.serializer = this.taskContext.<T>getInputSerializer(0).getSerializer();
        this.comparator = this.taskContext.getDriverComparator(0);
        this.input = this.taskContext.getInput(0);

        ExecutionConfig executionConfig = taskContext.getExecutionConfig();
        this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "ReduceDriver object reuse: "
                            + (this.objectReuseEnabled ? "ENABLED" : "DISABLED")
                            + ".");
        }
    }

    @Override
    public void run() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    this.taskContext.formatLogString(
                            "Reducer preprocessing done. Running Reducer code."));
        }

        final Counter numRecordsIn =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        final Counter numRecordsOut =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

        // cache references on the stack
        final MutableObjectIterator<T> input = this.input;
        final TypeSerializer<T> serializer = this.serializer;
        final TypeComparator<T> comparator = this.comparator;

        final ReduceFunction<T> function = this.taskContext.getStub();

        final Collector<T> output =
                new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

        if (objectReuseEnabled) {
            // We only need two objects. The first reference stores results and is
            // eventually collected. New values are read into the second.
            //
            // The output value must have the same key fields as the input values.

            T reuse1 = input.next();
            T reuse2 = serializer.createInstance();

            T value = reuse1;

            // iterate over key groups
            while (this.running && value != null) {
                numRecordsIn.inc();
                comparator.setReference(value);

                // iterate within a key group
                while ((reuse2 = input.next(reuse2)) != null) {
                    numRecordsIn.inc();
                    if (comparator.equalToReference(reuse2)) {
                        // same group, reduce
                        value = function.reduce(value, reuse2);

                        // we must never read into the object returned
                        // by the user, so swap the reuse objects
                        if (value == reuse2) {
                            T tmp = reuse1;
                            reuse1 = reuse2;
                            reuse2 = tmp;
                        }
                    } else {
                        // new key group
                        break;
                    }
                }

                output.collect(value);

                // swap the value from the new key group into the first object
                T tmp = reuse1;
                reuse1 = reuse2;
                reuse2 = tmp;

                value = reuse1;
            }
        } else {
            T value = input.next();

            // iterate over key groups
            while (this.running && value != null) {
                numRecordsIn.inc();
                comparator.setReference(value);
                T res = value;

                // iterate within a key group
                while ((value = input.next()) != null) {
                    numRecordsIn.inc();
                    if (comparator.equalToReference(value)) {
                        // same group, reduce
                        res = function.reduce(res, value);
                    } else {
                        // new key group
                        break;
                    }
                }

                output.collect(res);
            }
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void cancel() {
        this.running = false;
    }
}
