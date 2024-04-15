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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.util.JoinHashMap;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.disk.InputViewIterator;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.iterative.concurrent.BlockingBackChannel;
import org.apache.flink.runtime.iterative.concurrent.BlockingBackChannelBroker;
import org.apache.flink.runtime.iterative.concurrent.Broker;
import org.apache.flink.runtime.iterative.concurrent.IterationAggregatorBroker;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetBroker;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetUpdateBarrier;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetUpdateBarrierBroker;
import org.apache.flink.runtime.iterative.concurrent.SuperstepBarrier;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatch;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatchBroker;
import org.apache.flink.runtime.iterative.event.AllWorkersDoneEvent;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.iterative.event.WorkerDoneEvent;
import org.apache.flink.runtime.iterative.io.SerializedUpdateBuffer;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.runtime.operators.hash.CompactingHashTable;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The head is responsible for coordinating an iteration and can run a {@link Driver} inside. It
 * will read the initial input and establish a {@link BlockingBackChannel} to the iteration's tail.
 * After successfully processing the input, it will send EndOfSuperstep events to its outputs. It
 * must also be connected to a synchronization task and after each superstep, it will wait until it
 * receives an {@link AllWorkersDoneEvent} from the sync, which signals that all other heads have
 * also finished their iteration. Starting with the second iteration, the input for the head is the
 * output of the tail, transmitted through the backchannel. Once the iteration is done, the head
 * will send a {@link TerminationEvent} to all it's connected tasks, signaling them to shutdown.
 *
 * <p>Assumption on the ordering of the outputs: - The first n output gates write to channels that
 * go to the tasks of the step function. - The next m output gates to the tasks that consume the
 * final solution. - The last output gate connects to the synchronization task.
 *
 * @param <X> The type of the bulk partial solution / solution set and the final output.
 * @param <Y> The type of the feed-back data set (bulk partial solution / workset). For bulk
 *     iterations, {@code Y} is the same as {@code X}
 */
public class IterationHeadTask<X, Y, S extends Function, OT> extends AbstractIterativeTask<S, OT> {

    private static final Logger log = LoggerFactory.getLogger(IterationHeadTask.class);

    private Collector<X> finalOutputCollector;

    private TypeSerializerFactory<Y> feedbackTypeSerializer;

    private TypeSerializerFactory<X> solutionTypeSerializer;

    private RecordWriter<IOReadableWritable> toSync;

    private ResultPartitionID toSyncPartitionId;

    private int feedbackDataInput; // workset or bulk partial solution

    // --------------------------------------------------------------------------------------------

    /**
     * Create an Invokable task and set its environment.
     *
     * @param environment The environment assigned to this invokable.
     */
    public IterationHeadTask(Environment environment) {
        super(environment);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected int getNumTaskInputs() {
        // this task has an additional input in the workset case for the initial solution set
        boolean isWorkset = config.getIsWorksetIteration();
        return driver.getNumberOfInputs() + (isWorkset ? 1 : 0);
    }

    @Override
    protected void initOutputs() throws Exception {
        // initialize the regular outputs first (the ones into the step function).
        super.initOutputs();

        // at this time, the outputs to the step function are created
        // add the outputs for the final solution
        List<RecordWriter<?>> finalOutputWriters = new ArrayList<RecordWriter<?>>();
        final TaskConfig finalOutConfig = this.config.getIterationHeadFinalOutputConfig();
        final ClassLoader userCodeClassLoader = getUserCodeClassLoader();
        this.finalOutputCollector =
                BatchTask.getOutputCollector(
                        this,
                        finalOutConfig,
                        userCodeClassLoader,
                        finalOutputWriters,
                        config.getNumOutputs(),
                        finalOutConfig.getNumOutputs());

        // sanity check the setup
        final int writersIntoStepFunction = this.eventualOutputs.size();
        final int writersIntoFinalResult = finalOutputWriters.size();
        final int syncGateIndex = this.config.getIterationHeadIndexOfSyncOutput();

        if (writersIntoStepFunction + writersIntoFinalResult != syncGateIndex) {
            throw new Exception(
                    "Error: Inconsistent head task setup - wrong mapping of output gates.");
        }
        // now, we can instantiate the sync gate
        this.toSync = new RecordWriterBuilder<>().build(getEnvironment().getWriter(syncGateIndex));
        this.toSyncPartitionId = getEnvironment().getWriter(syncGateIndex).getPartitionId();
    }

    /**
     * The iteration head prepares the backchannel: it allocates memory, instantiates a {@link
     * BlockingBackChannel} and hands it to the iteration tail via a {@link Broker} singleton.
     */
    private BlockingBackChannel initBackChannel() throws Exception {

        /* get the size of the memory available to the backchannel */
        int backChannelMemoryPages =
                getMemoryManager().computeNumberOfPages(this.config.getRelativeBackChannelMemory());

        /* allocate the memory available to the backchannel */
        List<MemorySegment> segments = new ArrayList<MemorySegment>();
        int segmentSize = getMemoryManager().getPageSize();
        getMemoryManager().allocatePages(this, segments, backChannelMemoryPages);

        /* instantiate the backchannel */
        BlockingBackChannel backChannel =
                new BlockingBackChannel(
                        new SerializedUpdateBuffer(segments, segmentSize, getIOManager()));

        /* hand the backchannel over to the iteration tail */
        Broker<BlockingBackChannel> broker = BlockingBackChannelBroker.instance();
        broker.handIn(brokerKey(), backChannel);

        return backChannel;
    }

    private <BT> CompactingHashTable<BT> initCompactingHashTable() throws Exception {
        // get some memory
        double hashjoinMemorySize = config.getRelativeSolutionSetMemory();
        final ClassLoader userCodeClassLoader = getUserCodeClassLoader();

        TypeSerializerFactory<BT> solutionTypeSerializerFactory =
                config.getSolutionSetSerializer(userCodeClassLoader);
        TypeComparatorFactory<BT> solutionTypeComparatorFactory =
                config.getSolutionSetComparator(userCodeClassLoader);

        TypeSerializer<BT> solutionTypeSerializer = solutionTypeSerializerFactory.getSerializer();
        TypeComparator<BT> solutionTypeComparator =
                solutionTypeComparatorFactory.createComparator();

        CompactingHashTable<BT> hashTable = null;
        List<MemorySegment> memSegments = null;
        boolean success = false;
        try {
            int numPages = getMemoryManager().computeNumberOfPages(hashjoinMemorySize);
            memSegments = getMemoryManager().allocatePages(getContainingTask(), numPages);
            hashTable =
                    new CompactingHashTable<BT>(
                            solutionTypeSerializer, solutionTypeComparator, memSegments);
            success = true;
            return hashTable;
        } finally {
            if (!success) {
                if (hashTable != null) {
                    try {
                        hashTable.close();
                    } catch (Throwable t) {
                        log.error(
                                "Error closing the solution set hash table after unsuccessful creation.",
                                t);
                    }
                }
                if (memSegments != null) {
                    try {
                        getMemoryManager().release(memSegments);
                    } catch (Throwable t) {
                        log.error(
                                "Error freeing memory after error during solution set hash table creation.",
                                t);
                    }
                }
            }
        }
    }

    private <BT> JoinHashMap<BT> initJoinHashMap() {
        TypeSerializerFactory<BT> solutionTypeSerializerFactory =
                config.getSolutionSetSerializer(getUserCodeClassLoader());
        TypeComparatorFactory<BT> solutionTypeComparatorFactory =
                config.getSolutionSetComparator(getUserCodeClassLoader());

        TypeSerializer<BT> solutionTypeSerializer = solutionTypeSerializerFactory.getSerializer();
        TypeComparator<BT> solutionTypeComparator =
                solutionTypeComparatorFactory.createComparator();

        return new JoinHashMap<BT>(solutionTypeSerializer, solutionTypeComparator);
    }

    private void readInitialSolutionSet(
            CompactingHashTable<X> solutionSet, MutableObjectIterator<X> solutionSetInput)
            throws IOException {
        solutionSet.open();
        solutionSet.buildTableWithUniqueKey(solutionSetInput);
    }

    private void readInitialSolutionSet(
            JoinHashMap<X> solutionSet, MutableObjectIterator<X> solutionSetInput)
            throws IOException {
        TypeSerializer<X> serializer = solutionTypeSerializer.getSerializer();

        X next;
        while ((next = solutionSetInput.next(serializer.createInstance())) != null) {
            solutionSet.insertOrReplace(next);
        }
    }

    private SuperstepBarrier initSuperstepBarrier() {
        SuperstepBarrier barrier = new SuperstepBarrier(getUserCodeClassLoader());
        TaskEventDispatcher taskEventDispatcher = getEnvironment().getTaskEventDispatcher();
        ResultPartitionID partitionId = toSyncPartitionId;
        taskEventDispatcher.subscribeToEvent(partitionId, barrier, AllWorkersDoneEvent.class);
        taskEventDispatcher.subscribeToEvent(partitionId, barrier, TerminationEvent.class);
        return barrier;
    }

    @Override
    public void run() throws Exception {
        final String brokerKey = brokerKey();
        final int workerIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();

        final boolean objectSolutionSet = config.isSolutionSetUnmanaged();

        CompactingHashTable<X> solutionSet = null; // if workset iteration
        JoinHashMap<X> solutionSetObjectMap =
                null; // if workset iteration with unmanaged solution set

        boolean waitForSolutionSetUpdate = config.getWaitForSolutionSetUpdate();
        boolean isWorksetIteration = config.getIsWorksetIteration();

        try {
            /* used for receiving the current iteration result from iteration tail */
            SuperstepKickoffLatch nextStepKickoff = new SuperstepKickoffLatch();
            SuperstepKickoffLatchBroker.instance().handIn(brokerKey, nextStepKickoff);

            BlockingBackChannel backChannel = initBackChannel();
            SuperstepBarrier barrier = initSuperstepBarrier();
            SolutionSetUpdateBarrier solutionSetUpdateBarrier = null;

            feedbackDataInput = config.getIterationHeadPartialSolutionOrWorksetInputIndex();
            feedbackTypeSerializer = this.getInputSerializer(feedbackDataInput);
            excludeFromReset(feedbackDataInput);

            int initialSolutionSetInput;
            if (isWorksetIteration) {
                initialSolutionSetInput = config.getIterationHeadSolutionSetInputIndex();
                solutionTypeSerializer = config.getSolutionSetSerializer(getUserCodeClassLoader());

                // setup the index for the solution set
                @SuppressWarnings("unchecked")
                MutableObjectIterator<X> solutionSetInput =
                        (MutableObjectIterator<X>)
                                createInputIterator(
                                        inputReaders[initialSolutionSetInput],
                                        solutionTypeSerializer);

                // read the initial solution set
                if (objectSolutionSet) {
                    solutionSetObjectMap = initJoinHashMap();
                    readInitialSolutionSet(solutionSetObjectMap, solutionSetInput);
                    SolutionSetBroker.instance().handIn(brokerKey, solutionSetObjectMap);
                } else {
                    solutionSet = initCompactingHashTable();
                    readInitialSolutionSet(solutionSet, solutionSetInput);
                    SolutionSetBroker.instance().handIn(brokerKey, solutionSet);
                }

                if (waitForSolutionSetUpdate) {
                    solutionSetUpdateBarrier = new SolutionSetUpdateBarrier();
                    SolutionSetUpdateBarrierBroker.instance()
                            .handIn(brokerKey, solutionSetUpdateBarrier);
                }
            } else {
                // bulk iteration case
                @SuppressWarnings("unchecked")
                TypeSerializerFactory<X> solSer = (TypeSerializerFactory<X>) feedbackTypeSerializer;
                solutionTypeSerializer = solSer;

                // = termination Criterion tail
                if (waitForSolutionSetUpdate) {
                    solutionSetUpdateBarrier = new SolutionSetUpdateBarrier();
                    SolutionSetUpdateBarrierBroker.instance()
                            .handIn(brokerKey, solutionSetUpdateBarrier);
                }
            }

            // instantiate all aggregators and register them at the iteration global registry
            RuntimeAggregatorRegistry aggregatorRegistry =
                    new RuntimeAggregatorRegistry(
                            config.getIterationAggregators(getUserCodeClassLoader()));
            IterationAggregatorBroker.instance().handIn(brokerKey, aggregatorRegistry);

            DataInputView superstepResult = null;

            while (this.running && !terminationRequested()) {

                if (log.isInfoEnabled()) {
                    log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
                }

                barrier.setup();

                if (waitForSolutionSetUpdate) {
                    solutionSetUpdateBarrier.setup();
                }

                if (!inFirstIteration()) {
                    feedBackSuperstepResult(superstepResult);
                }

                super.run();

                // signal to connected tasks that we are done with the superstep
                sendEndOfSuperstepToAllIterationOutputs();

                if (waitForSolutionSetUpdate) {
                    solutionSetUpdateBarrier.waitForSolutionSetUpdate();
                }

                // blocking call to wait for the result
                superstepResult = backChannel.getReadEndAfterSuperstepEnded();
                if (log.isInfoEnabled()) {
                    log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
                }

                sendEventToSync(
                        new WorkerDoneEvent(workerIndex, aggregatorRegistry.getAllAggregators()));

                if (log.isInfoEnabled()) {
                    log.info(
                            formatLogString(
                                    "waiting for other workers in iteration ["
                                            + currentIteration()
                                            + "]"));
                }

                barrier.waitForOtherWorkers();

                if (barrier.terminationSignaled()) {
                    if (log.isInfoEnabled()) {
                        log.info(
                                formatLogString(
                                        "head received termination request in iteration ["
                                                + currentIteration()
                                                + "]"));
                    }
                    requestTermination();
                    nextStepKickoff.signalTermination();
                } else {
                    incrementIterationCounter();

                    String[] globalAggregateNames = barrier.getAggregatorNames();
                    Value[] globalAggregates = barrier.getAggregates();
                    aggregatorRegistry.updateGlobalAggregatesAndReset(
                            globalAggregateNames, globalAggregates);

                    nextStepKickoff.triggerNextSuperstep();
                }
            }

            if (log.isInfoEnabled()) {
                log.info(
                        formatLogString(
                                "streaming out final result after ["
                                        + currentIteration()
                                        + "] iterations"));
            }

            if (isWorksetIteration) {
                if (objectSolutionSet) {
                    streamSolutionSetToFinalOutput(solutionSetObjectMap);
                } else {
                    streamSolutionSetToFinalOutput(solutionSet);
                }
            } else {
                streamOutFinalOutputBulk(
                        new InputViewIterator<X>(
                                superstepResult, this.solutionTypeSerializer.getSerializer()));
            }

            this.finalOutputCollector.close();

        } finally {
            // make sure we unregister everything from the broker:
            // - backchannel
            // - aggregator registry
            // - solution set index
            IterationAggregatorBroker.instance().remove(brokerKey);
            BlockingBackChannelBroker.instance().remove(brokerKey);
            SuperstepKickoffLatchBroker.instance().remove(brokerKey);
            SolutionSetBroker.instance().remove(brokerKey);
            SolutionSetUpdateBarrierBroker.instance().remove(brokerKey);

            if (solutionSet != null) {
                solutionSet.close();
            }
        }
    }

    private void streamOutFinalOutputBulk(MutableObjectIterator<X> results) throws IOException {
        final Collector<X> out = this.finalOutputCollector;
        X record = this.solutionTypeSerializer.getSerializer().createInstance();

        while ((record = results.next(record)) != null) {
            out.collect(record);
        }
    }

    private void streamSolutionSetToFinalOutput(CompactingHashTable<X> hashTable)
            throws IOException {
        final MutableObjectIterator<X> results = hashTable.getEntryIterator();
        final Collector<X> output = this.finalOutputCollector;
        X record = solutionTypeSerializer.getSerializer().createInstance();

        while ((record = results.next(record)) != null) {
            output.collect(record);
        }
    }

    @SuppressWarnings("unchecked")
    private void streamSolutionSetToFinalOutput(JoinHashMap<X> soluionSet) throws IOException {
        final Collector<X> output = this.finalOutputCollector;
        for (Object e : soluionSet.values()) {
            output.collect((X) e);
        }
    }

    private void feedBackSuperstepResult(DataInputView superstepResult) {
        this.inputs[this.feedbackDataInput] =
                new InputViewIterator<Y>(
                        superstepResult, this.feedbackTypeSerializer.getSerializer());
    }

    private void sendEndOfSuperstepToAllIterationOutputs()
            throws IOException, InterruptedException {
        if (log.isDebugEnabled()) {
            log.debug(formatLogString("Sending end-of-superstep to all iteration outputs."));
        }

        for (RecordWriter<?> eventualOutput : this.eventualOutputs) {
            eventualOutput.broadcastEvent(EndOfSuperstepEvent.INSTANCE);
        }
    }

    private void sendEventToSync(WorkerDoneEvent event) throws IOException, InterruptedException {
        if (log.isInfoEnabled()) {
            log.info(
                    formatLogString(
                            "sending " + WorkerDoneEvent.class.getSimpleName() + " to sync"));
        }
        this.toSync.broadcastEvent(event);
    }
}
