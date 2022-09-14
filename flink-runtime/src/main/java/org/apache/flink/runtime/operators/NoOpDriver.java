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
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A driver that does nothing but forward data from its input to its output.
 *
 * @param <T> The data type.
 */
public class NoOpDriver<T> implements Driver<AbstractRichFunction, T> {

    private static final Logger LOG = LoggerFactory.getLogger(NoOpDriver.class);

    private TaskContext<AbstractRichFunction, T> taskContext;

    private volatile boolean running;

    private boolean objectReuseEnabled = false;

    @Override
    public void setup(TaskContext<AbstractRichFunction, T> context) {
        this.taskContext = context;
        this.running = true;
    }

    @Override
    public int getNumberOfInputs() {
        return 1;
    }

    @Override
    public Class<AbstractRichFunction> getStubType() {
        return null;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 0;
    }

    @Override
    public void prepare() {
        ExecutionConfig executionConfig = taskContext.getExecutionConfig();
        this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "NoOpDriver object reuse: "
                            + (this.objectReuseEnabled ? "ENABLED" : "DISABLED")
                            + ".");
        }
    }

    @Override
    public void run() throws Exception {
        // cache references on the stack
        final Counter numRecordsIn =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        final Counter numRecordsOut =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        final MutableObjectIterator<T> input = this.taskContext.getInput(0);
        final Collector<T> output =
                new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

        if (objectReuseEnabled) {
            T record = this.taskContext.<T>getInputSerializer(0).getSerializer().createInstance();

            while (this.running && ((record = input.next(record)) != null)) {
                numRecordsIn.inc();
                output.collect(record);
            }
        } else {
            T record;
            while (this.running && ((record = input.next()) != null)) {
                numRecordsIn.inc();
                output.collect(record);
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
