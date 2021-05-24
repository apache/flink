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
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.sort.NonReusingSortMergeCoGroupIterator;
import org.apache.flink.runtime.operators.sort.ReusingSortMergeCoGroupIterator;
import org.apache.flink.runtime.operators.util.CoGroupTaskIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.operators.util.metrics.CountingMutableObjectIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CoGroup task which is executed by a Task Manager. The task has two inputs and one or multiple
 * outputs. It is provided with a CoGroupFunction implementation.
 *
 * <p>The CoGroupTask group all pairs that share the same key from both inputs. Each for each key,
 * the sets of values that were pair with that key of both inputs are handed to the <code>coGroup()
 * </code> method of the CoGroupFunction.
 *
 * @see org.apache.flink.api.common.functions.CoGroupFunction
 */
public class CoGroupDriver<IT1, IT2, OT> implements Driver<CoGroupFunction<IT1, IT2, OT>, OT> {

    private static final Logger LOG = LoggerFactory.getLogger(CoGroupDriver.class);

    private TaskContext<CoGroupFunction<IT1, IT2, OT>, OT> taskContext;

    private CoGroupTaskIterator<IT1, IT2>
            coGroupIterator; // the iterator that does the actual cogroup

    private volatile boolean running;

    private boolean objectReuseEnabled = false;

    // ------------------------------------------------------------------------

    @Override
    public void setup(TaskContext<CoGroupFunction<IT1, IT2, OT>, OT> context) {
        this.taskContext = context;
        this.running = true;
    }

    @Override
    public int getNumberOfInputs() {
        return 2;
    }

    @Override
    public Class<CoGroupFunction<IT1, IT2, OT>> getStubType() {
        @SuppressWarnings("unchecked")
        final Class<CoGroupFunction<IT1, IT2, OT>> clazz =
                (Class<CoGroupFunction<IT1, IT2, OT>>) (Class<?>) CoGroupFunction.class;
        return clazz;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 2;
    }

    @Override
    public void prepare() throws Exception {
        final TaskConfig config = this.taskContext.getTaskConfig();
        if (config.getDriverStrategy() != DriverStrategy.CO_GROUP) {
            throw new Exception(
                    "Unrecognized driver strategy for CoGoup driver: "
                            + config.getDriverStrategy().name());
        }

        final Counter numRecordsIn =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();

        final MutableObjectIterator<IT1> in1 =
                new CountingMutableObjectIterator<>(
                        this.taskContext.<IT1>getInput(0), numRecordsIn);
        final MutableObjectIterator<IT2> in2 =
                new CountingMutableObjectIterator<>(
                        this.taskContext.<IT2>getInput(1), numRecordsIn);

        // get the key positions and types
        final TypeSerializer<IT1> serializer1 =
                this.taskContext.<IT1>getInputSerializer(0).getSerializer();
        final TypeSerializer<IT2> serializer2 =
                this.taskContext.<IT2>getInputSerializer(1).getSerializer();
        final TypeComparator<IT1> groupComparator1 = this.taskContext.getDriverComparator(0);
        final TypeComparator<IT2> groupComparator2 = this.taskContext.getDriverComparator(1);

        final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory =
                config.getPairComparatorFactory(this.taskContext.getUserCodeClassLoader());
        if (pairComparatorFactory == null) {
            throw new Exception("Missing pair comparator factory for CoGroup driver");
        }

        ExecutionConfig executionConfig = taskContext.getExecutionConfig();
        this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "CoGroupDriver object reuse: "
                            + (this.objectReuseEnabled ? "ENABLED" : "DISABLED")
                            + ".");
        }

        if (objectReuseEnabled) {
            // create CoGroupTaskIterator according to provided local strategy.
            this.coGroupIterator =
                    new ReusingSortMergeCoGroupIterator<IT1, IT2>(
                            in1,
                            in2,
                            serializer1,
                            groupComparator1,
                            serializer2,
                            groupComparator2,
                            pairComparatorFactory.createComparator12(
                                    groupComparator1, groupComparator2));
        } else {
            // create CoGroupTaskIterator according to provided local strategy.
            this.coGroupIterator =
                    new NonReusingSortMergeCoGroupIterator<IT1, IT2>(
                            in1,
                            in2,
                            serializer1,
                            groupComparator1,
                            serializer2,
                            groupComparator2,
                            pairComparatorFactory.createComparator12(
                                    groupComparator1, groupComparator2));
        }

        // open CoGroupTaskIterator - this triggers the sorting and blocks until the iterator is
        // ready
        this.coGroupIterator.open();

        if (LOG.isDebugEnabled()) {
            LOG.debug(this.taskContext.formatLogString("CoGroup task iterator ready."));
        }
    }

    @Override
    public void run() throws Exception {
        final Counter numRecordsOut =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

        final CoGroupFunction<IT1, IT2, OT> coGroupStub = this.taskContext.getStub();
        final Collector<OT> collector =
                new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);
        final CoGroupTaskIterator<IT1, IT2> coGroupIterator = this.coGroupIterator;

        while (this.running && coGroupIterator.next()) {
            coGroupStub.coGroup(
                    coGroupIterator.getValues1(), coGroupIterator.getValues2(), collector);
        }
    }

    @Override
    public void cleanup() throws Exception {
        if (this.coGroupIterator != null) {
            this.coGroupIterator.close();
            this.coGroupIterator = null;
        }
    }

    @Override
    public void cancel() throws Exception {
        this.running = false;
        cleanup();
    }
}
