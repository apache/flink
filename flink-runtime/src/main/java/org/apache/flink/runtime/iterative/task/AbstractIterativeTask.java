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

package org.apache.flink.runtime.iterative.task;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.operators.util.JoinHashMap;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.iterative.concurrent.BlockingBackChannel;
import org.apache.flink.runtime.iterative.concurrent.BlockingBackChannelBroker;
import org.apache.flink.runtime.iterative.concurrent.Broker;
import org.apache.flink.runtime.iterative.concurrent.IterationAggregatorBroker;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetBroker;
import org.apache.flink.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import org.apache.flink.runtime.iterative.io.SolutionSetObjectsUpdateOutputCollector;
import org.apache.flink.runtime.iterative.io.SolutionSetUpdateOutputCollector;
import org.apache.flink.runtime.iterative.io.WorksetUpdateOutputCollector;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.runtime.operators.ResettableDriver;
import org.apache.flink.runtime.operators.hash.CompactingHashTable;
import org.apache.flink.runtime.operators.util.DistributedRuntimeUDFContext;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.UserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/** The abstract base class for all tasks able to participate in an iteration. */
public abstract class AbstractIterativeTask<S extends Function, OT> extends BatchTask<S, OT>
        implements Terminable {

    private static final Logger log = LoggerFactory.getLogger(AbstractIterativeTask.class);

    protected LongSumAggregator worksetAggregator;

    protected BlockingBackChannel worksetBackChannel;

    protected boolean isWorksetIteration;

    protected boolean isWorksetUpdate;

    protected boolean isSolutionSetUpdate;

    private RuntimeAggregatorRegistry iterationAggregators;

    private String brokerKey;

    private int superstepNum = 1;

    private volatile boolean terminationRequested;

    private final CompletableFuture<Void> terminationCompletionFuture = new CompletableFuture<>();

    // --------------------------------------------------------------------------------------------

    /**
     * Create an Invokable task and set its environment.
     *
     * @param environment The environment assigned to this invokable.
     */
    public AbstractIterativeTask(Environment environment) {
        super(environment);
    }

    // --------------------------------------------------------------------------------------------
    // Main life cycle methods that implement the iterative behavior
    // --------------------------------------------------------------------------------------------

    @Override
    protected void initialize() throws Exception {
        super.initialize();

        // check if the driver is resettable
        if (this.driver instanceof ResettableDriver) {
            final ResettableDriver<?, ?> resDriver = (ResettableDriver<?, ?>) this.driver;
            // make sure that the according inputs are not reset
            for (int i = 0; i < resDriver.getNumberOfInputs(); i++) {
                if (resDriver.isInputResettable(i)) {
                    excludeFromReset(i);
                }
            }
        }

        TaskConfig config = getLastTasksConfig();
        isWorksetIteration = config.getIsWorksetIteration();
        isWorksetUpdate = config.getIsWorksetUpdate();
        isSolutionSetUpdate = config.getIsSolutionSetUpdate();

        if (isWorksetUpdate) {
            worksetBackChannel = BlockingBackChannelBroker.instance().getAndRemove(brokerKey());

            if (isWorksetIteration) {
                worksetAggregator =
                        getIterationAggregators()
                                .getAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME);

                if (worksetAggregator == null) {
                    throw new RuntimeException("Missing workset elements count aggregator.");
                }
            }
        }
    }

    @Override
    public void run() throws Exception {
        if (inFirstIteration()) {
            if (this.driver instanceof ResettableDriver) {
                // initialize the repeatable driver
                ((ResettableDriver<?, ?>) this.driver).initialize();
            }
        } else {
            reinstantiateDriver();
            resetAllInputs();

            // re-read the iterative broadcast variables
            for (int i : this.iterativeBroadcastInputs) {
                final String name = getTaskConfig().getBroadcastInputName(i);
                readAndSetBroadcastInput(i, name, this.runtimeUdfContext, superstepNum);
            }
        }

        // call the parent to execute the superstep
        super.run();

        // release the iterative broadcast variables
        for (int i : this.iterativeBroadcastInputs) {
            final String name = getTaskConfig().getBroadcastInputName(i);
            releaseBroadcastVariables(name, superstepNum, this.runtimeUdfContext);
        }
    }

    @Override
    protected void closeLocalStrategiesAndCaches() {
        try {
            super.closeLocalStrategiesAndCaches();
        } finally {
            if (this.driver instanceof ResettableDriver) {
                final ResettableDriver<?, ?> resDriver = (ResettableDriver<?, ?>) this.driver;
                try {
                    resDriver.teardown();
                } catch (Throwable t) {
                    log.error("Error while shutting down an iterative operator.", t);
                }
            }
        }
    }

    @Override
    public DistributedRuntimeUDFContext createRuntimeContext(OperatorMetricGroup metrics) {
        Environment env = getEnvironment();

        return new IterativeRuntimeUdfContext(
                env.getTaskInfo(),
                env.getUserCodeClassLoader(),
                getExecutionConfig(),
                env.getDistributedCacheEntries(),
                this.accumulatorMap,
                metrics,
                env.getExternalResourceInfoProvider(),
                env.getJobID());
    }

    // --------------------------------------------------------------------------------------------
    // Utility Methods for Iteration Handling
    // --------------------------------------------------------------------------------------------

    protected boolean inFirstIteration() {
        return this.superstepNum == 1;
    }

    protected int currentIteration() {
        return this.superstepNum;
    }

    protected void incrementIterationCounter() {
        this.superstepNum++;
    }

    public String brokerKey() {
        if (brokerKey == null) {
            int iterationId = config.getIterationId();
            brokerKey =
                    getEnvironment().getJobID().toString()
                            + '#'
                            + iterationId
                            + '#'
                            + getEnvironment().getTaskInfo().getIndexOfThisSubtask();
        }
        return brokerKey;
    }

    private void reinstantiateDriver() throws Exception {
        if (this.driver instanceof ResettableDriver) {
            final ResettableDriver<?, ?> resDriver = (ResettableDriver<?, ?>) this.driver;
            resDriver.reset();
        } else {
            Class<? extends Driver<S, OT>> driverClass = this.config.getDriver();
            this.driver = InstantiationUtil.instantiate(driverClass, Driver.class);

            try {
                this.driver.setup(this);
            } catch (Throwable t) {
                throw new Exception(
                        "The pact driver setup for '"
                                + this.getEnvironment().getTaskInfo().getTaskName()
                                + "' , caused an error: "
                                + t.getMessage(),
                        t);
            }
        }
    }

    public RuntimeAggregatorRegistry getIterationAggregators() {
        if (this.iterationAggregators == null) {
            this.iterationAggregators = IterationAggregatorBroker.instance().get(brokerKey());
        }
        return this.iterationAggregators;
    }

    protected void verifyEndOfSuperstepState() throws IOException {
        // sanity check that there is at least one iterative input reader
        if (this.iterativeInputs.length == 0 && this.iterativeBroadcastInputs.length == 0) {
            throw new IllegalStateException(
                    "Error: Iterative task without a single iterative input.");
        }

        for (int inputNum : this.iterativeInputs) {
            MutableReader<?> reader = this.inputReaders[inputNum];

            if (!reader.isFinished()) {
                if (reader.hasReachedEndOfSuperstep()) {
                    reader.startNextSuperstep();
                } else {
                    // need to read and drop all non-consumed data until we reach the
                    // end-of-superstep
                    @SuppressWarnings("unchecked")
                    MutableObjectIterator<Object> inIter =
                            (MutableObjectIterator<Object>) this.inputIterators[inputNum];
                    Object o = this.inputSerializers[inputNum].getSerializer().createInstance();
                    while ((o = inIter.next(o)) != null) {}

                    if (!reader.isFinished()) {
                        // also reset the end-of-superstep state
                        reader.startNextSuperstep();
                    }
                }
            }
        }

        for (int inputNum : this.iterativeBroadcastInputs) {
            MutableReader<?> reader = this.broadcastInputReaders[inputNum];

            if (!reader.isFinished()) {

                // sanity check that the BC input is at the end of the superstep
                if (!reader.hasReachedEndOfSuperstep()) {
                    throw new IllegalStateException(
                            "An iterative broadcast input has not been fully consumed.");
                }

                reader.startNextSuperstep();
            }
        }
    }

    @Override
    public boolean terminationRequested() {
        return this.terminationRequested;
    }

    @Override
    public void requestTermination() {
        this.terminationRequested = true;
    }

    @Override
    public void terminationCompleted() {
        this.terminationCompletionFuture.complete(null);
    }

    @Override
    public Future<Void> cancel() throws Exception {
        requestTermination();
        return this.terminationCompletionFuture;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Iteration State Update Handling
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new {@link WorksetUpdateOutputCollector}.
     *
     * <p>This collector is used by {@link IterationIntermediateTask} or {@link IterationTailTask}
     * to update the workset.
     *
     * <p>If a non-null delegate is given, the new {@link Collector} will write to the solution set
     * and also call collect(T) of the delegate.
     *
     * @param delegate null -OR- the delegate on which to call collect() by the newly created
     *     collector
     * @return a new {@link WorksetUpdateOutputCollector}
     */
    protected Collector<OT> createWorksetUpdateOutputCollector(Collector<OT> delegate) {
        DataOutputView outputView = worksetBackChannel.getWriteEnd();
        TypeSerializer<OT> serializer = getOutputSerializer();
        return new WorksetUpdateOutputCollector<OT>(outputView, serializer, delegate);
    }

    protected Collector<OT> createWorksetUpdateOutputCollector() {
        return createWorksetUpdateOutputCollector(null);
    }

    /**
     * Creates a new solution set update output collector.
     *
     * <p>This collector is used by {@link IterationIntermediateTask} or {@link IterationTailTask}
     * to update the solution set of workset iterations. Depending on the task configuration, either
     * a fast (non-probing) {@link
     * org.apache.flink.runtime.iterative.io.SolutionSetFastUpdateOutputCollector} or normal
     * (re-probing) {@link SolutionSetUpdateOutputCollector} is created.
     *
     * <p>If a non-null delegate is given, the new {@link Collector} will write back to the solution
     * set and also call collect(T) of the delegate.
     *
     * @param delegate null -OR- a delegate collector to be called by the newly created collector
     * @return a new {@link
     *     org.apache.flink.runtime.iterative.io.SolutionSetFastUpdateOutputCollector} or {@link
     *     SolutionSetUpdateOutputCollector}
     */
    protected Collector<OT> createSolutionSetUpdateOutputCollector(Collector<OT> delegate) {
        Broker<Object> solutionSetBroker = SolutionSetBroker.instance();

        Object ss = solutionSetBroker.get(brokerKey());
        if (ss instanceof CompactingHashTable) {
            @SuppressWarnings("unchecked")
            CompactingHashTable<OT> solutionSet = (CompactingHashTable<OT>) ss;
            return new SolutionSetUpdateOutputCollector<OT>(solutionSet, delegate);
        } else if (ss instanceof JoinHashMap) {
            @SuppressWarnings("unchecked")
            JoinHashMap<OT> map = (JoinHashMap<OT>) ss;
            return new SolutionSetObjectsUpdateOutputCollector<OT>(map, delegate);
        } else {
            throw new RuntimeException("Unrecognized solution set handle: " + ss);
        }
    }

    /** @return output serializer of this task */
    private TypeSerializer<OT> getOutputSerializer() {
        TypeSerializerFactory<OT> serializerFactory;

        if ((serializerFactory = getLastTasksConfig().getOutputSerializer(getUserCodeClassLoader()))
                == null) {
            throw new RuntimeException("Missing output serializer for workset update.");
        }

        return serializerFactory.getSerializer();
    }

    // -----------------------------------------------------------------------------------------------------------------

    private class IterativeRuntimeUdfContext extends DistributedRuntimeUDFContext
            implements IterationRuntimeContext {

        public IterativeRuntimeUdfContext(
                TaskInfo taskInfo,
                UserCodeClassLoader userCodeClassLoader,
                ExecutionConfig executionConfig,
                Map<String, Future<Path>> cpTasks,
                Map<String, Accumulator<?, ?>> accumulatorMap,
                OperatorMetricGroup metrics,
                ExternalResourceInfoProvider externalResourceInfoProvider,
                JobID jobID) {
            super(
                    taskInfo,
                    userCodeClassLoader,
                    executionConfig,
                    cpTasks,
                    accumulatorMap,
                    metrics,
                    externalResourceInfoProvider,
                    jobID);
        }

        @Override
        public int getSuperstepNumber() {
            return AbstractIterativeTask.this.superstepNum;
        }

        @Override
        public <T extends Aggregator<?>> T getIterationAggregator(String name) {
            return getIterationAggregators().<T>getAggregator(name);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Value> T getPreviousIterationAggregate(String name) {
            return (T) getIterationAggregators().getPreviousGlobalAggregate(name);
        }

        @Override
        public JobID getJobId() {
            return runtimeUdfContext.getJobId();
        }

        @Override
        public <V, A extends Serializable> void addAccumulator(
                String name, Accumulator<V, A> newAccumulator) {
            // only add accumulator on first iteration
            if (inFirstIteration()) {
                super.addAccumulator(name, newAccumulator);
            }
        }
    }
}
