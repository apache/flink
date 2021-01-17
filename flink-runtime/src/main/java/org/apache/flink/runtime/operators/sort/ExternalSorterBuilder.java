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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder for an {@link ExternalSorter}. It can construct either a pull-based sorter if provided
 * with an input iterator via {@link #build(MutableObjectIterator)} or a push-based one via {@link
 * #build()}.
 */
public final class ExternalSorterBuilder<T> {

    /** Logging. */
    private static final Logger LOG = LoggerFactory.getLogger(ExternalSorter.class);

    /**
     * Fix length records with a length below this threshold will be in-place sorted, if possible.
     */
    private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

    /** The minimal number of buffers to use by the writers. */
    private static final int MIN_NUM_WRITE_BUFFERS = 2;

    /** The maximal number of buffers to use by the writers. */
    private static final int MAX_NUM_WRITE_BUFFERS = 4;

    /** The minimum number of segments that are required for the sort to operate. */
    private static final int MIN_NUM_SORT_MEM_SEGMENTS = 10;

    private final MemoryManager memoryManager;
    private final AbstractInvokable parentTask;
    private final TypeSerializer<T> serializer;
    private final TypeComparator<T> comparator;
    private InMemorySorterFactory<T> inMemorySorterFactory;
    private int maxNumFileHandles = AlgorithmOptions.SPILLING_MAX_FAN.defaultValue();
    private boolean objectReuseEnabled = false;
    private boolean handleLargeRecords = ConfigConstants.DEFAULT_USE_LARGE_RECORD_HANDLER;
    private double memoryFraction = 1.0;
    private int numSortBuffers = -1;
    private double startSpillingFraction = AlgorithmOptions.SORT_SPILLING_THRESHOLD.defaultValue();
    private IOManager ioManager;
    private boolean noSpillingMemory = true;
    private GroupCombineFunction<T, T> combineFunction;
    private Configuration udfConfig;
    private List<MemorySegment> memorySegments = null;

    ExternalSorterBuilder(
            MemoryManager memoryManager,
            AbstractInvokable parentTask,
            TypeSerializer<T> serializer,
            TypeComparator<T> comparator) {
        this.memoryManager = memoryManager;
        this.parentTask = parentTask;
        this.serializer = serializer;
        this.comparator = comparator;
        this.inMemorySorterFactory =
                new DefaultInMemorySorterFactory<>(
                        serializer, comparator, THRESHOLD_FOR_IN_PLACE_SORTING);
    }

    public ExternalSorterBuilder<T> maxNumFileHandles(int maxNumFileHandles) {
        if (maxNumFileHandles < 2) {
            throw new IllegalArgumentException(
                    "Merger cannot work with less than two file handles.");
        }
        this.maxNumFileHandles = maxNumFileHandles;
        return this;
    }

    public ExternalSorterBuilder<T> objectReuse(boolean enabled) {
        this.objectReuseEnabled = enabled;
        return this;
    }

    public ExternalSorterBuilder<T> largeRecords(boolean enabled) {
        this.handleLargeRecords = enabled;
        return this;
    }

    public ExternalSorterBuilder<T> enableSpilling(IOManager ioManager) {
        this.noSpillingMemory = false;
        this.ioManager = checkNotNull(ioManager);
        return this;
    }

    public ExternalSorterBuilder<T> enableSpilling(
            IOManager ioManager, double startSpillingFraction) {
        this.startSpillingFraction = startSpillingFraction;
        return enableSpilling(ioManager);
    }

    public ExternalSorterBuilder<T> memoryFraction(double fraction) {
        this.memoryFraction = fraction;
        return this;
    }

    public ExternalSorterBuilder<T> memory(List<MemorySegment> memorySegments) {
        this.memorySegments = checkNotNull(memorySegments);
        return this;
    }

    public ExternalSorterBuilder<T> sortBuffers(int numSortBuffers) {
        this.numSortBuffers = numSortBuffers;
        return this;
    }

    public ExternalSorterBuilder<T> withCombiner(
            GroupCombineFunction<T, T> combineFunction, Configuration udfConfig) {
        this.combineFunction = checkNotNull(combineFunction);
        this.udfConfig = checkNotNull(udfConfig);
        return this;
    }

    public ExternalSorterBuilder<T> withCombiner(GroupCombineFunction<T, T> combineFunction) {
        this.combineFunction = checkNotNull(combineFunction);
        this.udfConfig = new Configuration();
        return this;
    }

    public ExternalSorterBuilder<T> sorterFactory(InMemorySorterFactory<T> sorterFactory) {
        this.inMemorySorterFactory = checkNotNull(sorterFactory);
        return this;
    }

    /**
     * Creates a pull-based {@link Sorter}. The {@link Sorter#getIterator()} will return when all
     * the records from the given input are consumed. Will spawn three threads: read, sort, spill.
     */
    public ExternalSorter<T> build(MutableObjectIterator<T> input)
            throws MemoryAllocationException {
        return doBuild(
                (exceptionHandler, dispatcher, largeRecordHandler, startSpillingBytes) ->
                        new ReadingThread<>(
                                exceptionHandler,
                                input,
                                dispatcher,
                                largeRecordHandler,
                                serializer.createInstance(),
                                startSpillingBytes));
    }

    /**
     * Creates a push-based {@link PushSorter}. The {@link PushSorter#getIterator()} will return
     * when the {@link PushSorter#finishReading()} is called. Will spawn two threads: sort, spill.
     */
    public PushSorter<T> build() throws MemoryAllocationException {
        PushFactory<T> pushFactory = new PushFactory<>();
        ExternalSorter<T> tExternalSorter = doBuild(pushFactory);

        return new PushSorter<T>() {
            private final SorterInputGateway<T> recordProducer = pushFactory.sorterInputGateway;

            @Override
            public void writeRecord(T record) throws IOException, InterruptedException {
                recordProducer.writeRecord(record);
            }

            @Override
            public void finishReading() {
                recordProducer.finishReading();
            }

            @Override
            public MutableObjectIterator<T> getIterator() throws InterruptedException {
                return tExternalSorter.getIterator();
            }

            @Override
            public void close() {
                tExternalSorter.close();
            }
        };
    }

    @FunctionalInterface
    private interface ReadingStageFactory<E> {
        @Nullable
        StageRunner createReadingStage(
                ExceptionHandler<IOException> exceptionHandler,
                StageRunner.StageMessageDispatcher<E> dispatcher,
                LargeRecordHandler<E> largeRecordHandler,
                long startSpillingBytes);
    }

    private static final class PushFactory<E> implements ReadingStageFactory<E> {
        private SorterInputGateway<E> sorterInputGateway;

        @Override
        public StageRunner createReadingStage(
                ExceptionHandler<IOException> exceptionHandler,
                StageRunner.StageMessageDispatcher<E> dispatcher,
                LargeRecordHandler<E> largeRecordHandler,
                long startSpillingBytes) {
            sorterInputGateway =
                    new SorterInputGateway<>(dispatcher, largeRecordHandler, startSpillingBytes);
            return null;
        }
    }

    private ExternalSorter<T> doBuild(ReadingStageFactory<T> readingStageFactory)
            throws MemoryAllocationException {

        final List<MemorySegment> memory;
        if (this.memorySegments != null) {
            memory = this.memorySegments;
        } else {
            memory =
                    memoryManager.allocatePages(
                            parentTask, memoryManager.computeNumberOfPages(memoryFraction));
        }

        // adjust the memory quotas to the page size
        final int numPagesTotal = memory.size();

        if (numPagesTotal < MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS) {
            throw new IllegalArgumentException(
                    "Too little memory provided to sorter to perform task. "
                            + "Required are at least "
                            + (MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS)
                            + " pages. Current page size is "
                            + memoryManager.getPageSize()
                            + " bytes.");
        }

        // determine how many buffers to use for writing
        final int numWriteBuffers;
        final int numLargeRecordBuffers;

        if (noSpillingMemory && !handleLargeRecords) {
            numWriteBuffers = 0;
            numLargeRecordBuffers = 0;
        } else {
            int numConsumers = (noSpillingMemory ? 0 : 1) + (handleLargeRecords ? 2 : 0);

            // determine how many buffers we have when we do a full merge with maximal fan-in
            final int minBuffersForMerging =
                    maxNumFileHandles + numConsumers * MIN_NUM_WRITE_BUFFERS;

            if (minBuffersForMerging > numPagesTotal) {
                numWriteBuffers = noSpillingMemory ? 0 : MIN_NUM_WRITE_BUFFERS;
                numLargeRecordBuffers = handleLargeRecords ? 2 * MIN_NUM_WRITE_BUFFERS : 0;

                maxNumFileHandles = numPagesTotal - numConsumers * MIN_NUM_WRITE_BUFFERS;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Reducing maximal merge fan-in to "
                                    + maxNumFileHandles
                                    + " due to limited memory availability during merge");
                }
            } else {
                // we are free to choose. make sure that we do not eat up too much memory for
                // writing
                final int fractionalAuxBuffers = numPagesTotal / (numConsumers * 100);

                if (fractionalAuxBuffers >= MAX_NUM_WRITE_BUFFERS) {
                    numWriteBuffers = noSpillingMemory ? 0 : MAX_NUM_WRITE_BUFFERS;
                    numLargeRecordBuffers = handleLargeRecords ? 2 * MAX_NUM_WRITE_BUFFERS : 0;
                } else {
                    numWriteBuffers =
                            noSpillingMemory
                                    ? 0
                                    : Math.max(
                                            MIN_NUM_WRITE_BUFFERS,
                                            fractionalAuxBuffers); // at least the lower bound

                    numLargeRecordBuffers = handleLargeRecords ? 2 * MIN_NUM_WRITE_BUFFERS : 0;
                }
            }
        }

        final int sortMemPages = numPagesTotal - numWriteBuffers - numLargeRecordBuffers;
        final long sortMemory = ((long) sortMemPages) * memoryManager.getPageSize();

        // decide how many sort buffers to use
        if (numSortBuffers < 1) {
            if (sortMemory > 100 * 1024 * 1024) {
                numSortBuffers = 2;
            } else {
                numSortBuffers = 1;
            }
        }
        final int numSegmentsPerSortBuffer = sortMemPages / numSortBuffers;

        LOG.debug(
                String.format(
                        "Instantiating sorter with %d pages of sorting memory (="
                                + "%d bytes total) divided over %d sort buffers (%d pages per buffer). Using %d"
                                + " buffers for writing sorted results and merging maximally %d streams at once. "
                                + "Using %d memory segments for large record spilling.",
                        sortMemPages,
                        sortMemory,
                        numSortBuffers,
                        numSegmentsPerSortBuffer,
                        numWriteBuffers,
                        maxNumFileHandles,
                        numLargeRecordBuffers));

        List<MemorySegment> writeMemory = new ArrayList<>(numWriteBuffers);

        LargeRecordHandler<T> largeRecordHandler;
        // move some pages from the sort memory to the write memory
        if (numWriteBuffers > 0) {
            for (int i = 0; i < numWriteBuffers; i++) {
                writeMemory.add(memory.remove(memory.size() - 1));
            }
        }
        if (numLargeRecordBuffers > 0) {
            List<MemorySegment> mem = new ArrayList<>();
            for (int i = 0; i < numLargeRecordBuffers; i++) {
                mem.add(memory.remove(memory.size() - 1));
            }

            largeRecordHandler =
                    new LargeRecordHandler<>(
                            serializer,
                            comparator.duplicate(),
                            ioManager,
                            memoryManager,
                            mem,
                            parentTask,
                            maxNumFileHandles);
        } else {
            largeRecordHandler = null;
        }

        // circular queues pass buffers between the threads
        final CircularQueues<T> circularQueues = new CircularQueues<>();

        final List<InMemorySorter<T>> inMemorySorters = new ArrayList<>(numSortBuffers);

        // allocate the sort buffers and fill empty queue with them
        final Iterator<MemorySegment> segments = memory.iterator();
        for (int i = 0; i < numSortBuffers; i++) {
            // grab some memory
            final List<MemorySegment> sortSegments = new ArrayList<>(numSegmentsPerSortBuffer);
            for (int k = (i == numSortBuffers - 1 ? Integer.MAX_VALUE : numSegmentsPerSortBuffer);
                    k > 0 && segments.hasNext();
                    k--) {
                sortSegments.add(segments.next());
            }

            final InMemorySorter<T> inMemorySorter = inMemorySorterFactory.create(sortSegments);
            inMemorySorters.add(inMemorySorter);

            // add to empty queue
            CircularElement<T> element = new CircularElement<>(i, inMemorySorter, sortSegments);
            circularQueues.send(StageRunner.SortStage.READ, element);
        }

        // exception handling
        ExceptionHandler<IOException> exceptionHandler =
                exception -> circularQueues.getIteratorFuture().completeExceptionally(exception);

        SpillChannelManager spillChannelManager = new SpillChannelManager();

        // start the thread that reads the input channels
        StageRunner readingThread =
                readingStageFactory.createReadingStage(
                        exceptionHandler,
                        circularQueues,
                        largeRecordHandler,
                        ((long) (startSpillingFraction * sortMemory)));

        // start the thread that sorts the buffers
        StageRunner sortingStage = new SortingThread<>(exceptionHandler, circularQueues);

        // start the thread that handles spilling to secondary storage
        final SpillingThread.SpillingBehaviour<T> spillingBehaviour;
        if (combineFunction != null) {
            spillingBehaviour =
                    new CombiningSpillingBehaviour<>(
                            combineFunction, serializer, comparator, objectReuseEnabled, udfConfig);
        } else {
            spillingBehaviour = new DefaultSpillingBehaviour<>(objectReuseEnabled, serializer);
        }

        StageRunner spillingStage =
                new SpillingThread<>(
                        exceptionHandler,
                        circularQueues,
                        memoryManager,
                        ioManager,
                        serializer,
                        comparator,
                        memory,
                        writeMemory,
                        maxNumFileHandles,
                        spillChannelManager,
                        largeRecordHandler,
                        spillingBehaviour,
                        MIN_NUM_WRITE_BUFFERS,
                        MAX_NUM_WRITE_BUFFERS);

        return new ExternalSorter<>(
                readingThread,
                sortingStage,
                spillingStage,
                memory,
                writeMemory,
                memoryManager,
                largeRecordHandler,
                spillChannelManager,
                inMemorySorters,
                circularQueues);
    }
}
