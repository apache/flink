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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.util.JoinTaskIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.operators.util.metrics.CountingMutableObjectIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The abstract outer join driver implements the logic of an outer join operator at runtime. It
 * instantiates a sort-merge based strategy to find joining pairs of records or joining records with
 * null depending on the outer join type.
 *
 * @see FlatJoinFunction
 */
public abstract class AbstractOuterJoinDriver<IT1, IT2, OT>
        implements Driver<FlatJoinFunction<IT1, IT2, OT>, OT> {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractOuterJoinDriver.class);

    protected TaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> taskContext;

    protected volatile JoinTaskIterator<IT1, IT2, OT>
            outerJoinIterator; // the iterator that does the actual outer join
    protected volatile boolean running;

    // ------------------------------------------------------------------------

    @Override
    public void setup(TaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> context) {
        this.taskContext = context;
        this.running = true;
    }

    @Override
    public int getNumberOfInputs() {
        return 2;
    }

    @Override
    public Class<FlatJoinFunction<IT1, IT2, OT>> getStubType() {
        @SuppressWarnings("unchecked")
        final Class<FlatJoinFunction<IT1, IT2, OT>> clazz =
                (Class<FlatJoinFunction<IT1, IT2, OT>>) (Class<?>) FlatJoinFunction.class;
        return clazz;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 2;
    }

    @Override
    public void prepare() throws Exception {
        final TaskConfig config = this.taskContext.getTaskConfig();

        // obtain task manager's memory manager and I/O manager
        final MemoryManager memoryManager = this.taskContext.getMemoryManager();
        final IOManager ioManager = this.taskContext.getIOManager();

        // set up memory and I/O parameters
        final double driverMemFraction = config.getRelativeMemoryDriver();

        final DriverStrategy ls = config.getDriverStrategy();

        final Counter numRecordsIn =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        final MutableObjectIterator<IT1> in1 =
                new CountingMutableObjectIterator<>(
                        this.taskContext.<IT1>getInput(0), numRecordsIn);
        final MutableObjectIterator<IT2> in2 =
                new CountingMutableObjectIterator<>(
                        this.taskContext.<IT2>getInput(1), numRecordsIn);

        // get serializers and comparators
        final TypeSerializer<IT1> serializer1 =
                this.taskContext.<IT1>getInputSerializer(0).getSerializer();
        final TypeSerializer<IT2> serializer2 =
                this.taskContext.<IT2>getInputSerializer(1).getSerializer();
        final TypeComparator<IT1> comparator1 = this.taskContext.getDriverComparator(0);
        final TypeComparator<IT2> comparator2 = this.taskContext.getDriverComparator(1);

        final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory =
                config.getPairComparatorFactory(this.taskContext.getUserCodeClassLoader());

        if (pairComparatorFactory == null) {
            throw new Exception("Missing pair comparator factory for outer join driver");
        }

        ExecutionConfig executionConfig = taskContext.getExecutionConfig();
        boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Outer Join Driver object reuse: "
                            + (objectReuseEnabled ? "ENABLED" : "DISABLED")
                            + ".");
        }

        // create and return outer join iterator according to provided local strategy.
        if (objectReuseEnabled) {
            this.outerJoinIterator =
                    getReusingOuterJoinIterator(
                            ls,
                            in1,
                            in2,
                            serializer1,
                            comparator1,
                            serializer2,
                            comparator2,
                            pairComparatorFactory,
                            memoryManager,
                            ioManager,
                            driverMemFraction);
        } else {
            this.outerJoinIterator =
                    getNonReusingOuterJoinIterator(
                            ls,
                            in1,
                            in2,
                            serializer1,
                            comparator1,
                            serializer2,
                            comparator2,
                            pairComparatorFactory,
                            memoryManager,
                            ioManager,
                            driverMemFraction);
        }

        this.outerJoinIterator.open();

        if (LOG.isDebugEnabled()) {
            LOG.debug(this.taskContext.formatLogString("outer join task iterator ready."));
        }
    }

    @Override
    public void run() throws Exception {
        final Counter numRecordsOut =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

        final FlatJoinFunction<IT1, IT2, OT> joinStub = this.taskContext.getStub();
        final Collector<OT> collector =
                new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);
        final JoinTaskIterator<IT1, IT2, OT> outerJoinIterator = this.outerJoinIterator;

        while (this.running && outerJoinIterator.callWithNextKey(joinStub, collector)) {}
    }

    @Override
    public void cleanup() throws Exception {
        if (this.outerJoinIterator != null) {
            this.outerJoinIterator.close();
            this.outerJoinIterator = null;
        }
    }

    @Override
    public void cancel() {
        this.running = false;
        if (this.outerJoinIterator != null) {
            this.outerJoinIterator.abort();
        }
    }

    protected abstract JoinTaskIterator<IT1, IT2, OT> getReusingOuterJoinIterator(
            DriverStrategy driverStrategy,
            MutableObjectIterator<IT1> in1,
            MutableObjectIterator<IT2> in2,
            TypeSerializer<IT1> serializer1,
            TypeComparator<IT1> comparator1,
            TypeSerializer<IT2> serializer2,
            TypeComparator<IT2> comparator2,
            TypePairComparatorFactory<IT1, IT2> pairComparatorFactory,
            MemoryManager memoryManager,
            IOManager ioManager,
            double driverMemFraction)
            throws Exception;

    protected abstract JoinTaskIterator<IT1, IT2, OT> getNonReusingOuterJoinIterator(
            DriverStrategy driverStrategy,
            MutableObjectIterator<IT1> in1,
            MutableObjectIterator<IT2> in2,
            TypeSerializer<IT1> serializer1,
            TypeComparator<IT1> comparator1,
            TypeSerializer<IT2> serializer2,
            TypeComparator<IT2> comparator2,
            TypePairComparatorFactory<IT1, IT2> pairComparatorFactory,
            MemoryManager memoryManager,
            IOManager ioManager,
            double driverMemFraction)
            throws Exception;
}
