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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * A base for {@link SourceReader}s that read splits with one thread using one {@link SplitReader}.
 * The splits can be read either one after the other (like in a file source) or concurrently by
 * changing the subscription in the split reader (like in the Kafka Source).
 *
 * <p>To implement a source reader based on this class, implementors need to supply the following:
 *
 * <ul>
 *   <li>A {@link SplitReader}, which connects to the source and reads/polls data. The split reader
 *       gets notified whenever there is a new split. The split reader would read files, contain a
 *       Kafka or other source client, etc.
 *   <li>A {@link RecordEmitter} that takes a record from the Split Reader and updates the
 *       checkpointing state and converts it into the final form. For example for Kafka, the Record
 *       Emitter takes a {@code ConsumerRecord}, puts the offset information into state, transforms
 *       the records with the deserializers into the final type, and emits the record.
 *   <li>The class must override the methods to convert back and forth between the immutable splits
 *       ({@code SplitT}) and the mutable split state representation ({@code SplitStateT}).
 *   <li>Finally, the reader must decide what to do when it starts ({@link #start()}) or when a
 *       split is finished ({@link #onSplitFinished(java.util.Map)}).
 * </ul>
 *
 * @param <E> The type of the records (the raw type that typically contains checkpointing
 *     information).
 * @param <T> The final type of the records emitted by the source.
 * @param <SplitT> The type of the splits processed by the source.
 * @param <SplitStateT> The type of the mutable state per split.
 */
@PublicEvolving
public abstract class SingleThreadMultiplexSourceReaderBase<
                E, T, SplitT extends SourceSplit, SplitStateT>
        extends SourceReaderBase<E, T, SplitT, SplitStateT> {

    /**
     * The primary constructor for the source reader.
     *
     * <p>The reader will use a handover queue sized as configured via {@link
     * SourceReaderOptions#ELEMENT_QUEUE_CAPACITY}.
     */
    public SingleThreadMultiplexSourceReaderBase(
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        this(
                new FutureCompletingBlockingQueue<>(
                        config.getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY)),
                splitReaderSupplier,
                recordEmitter,
                config,
                context);
    }

    /**
     * This constructor behaves like {@link #SingleThreadMultiplexSourceReaderBase(Supplier,
     * RecordEmitter, Configuration, SourceReaderContext)}, but accepts a specific {@link
     * FutureCompletingBlockingQueue}.
     */
    public SingleThreadMultiplexSourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier, config),
                recordEmitter,
                config,
                context);
    }

    /**
     * This constructor behaves like {@link #SingleThreadMultiplexSourceReaderBase(Supplier,
     * RecordEmitter, Configuration, SourceReaderContext)}, but accepts a specific {@link
     * FutureCompletingBlockingQueue} and {@link SingleThreadFetcherManager}.
     */
    public SingleThreadMultiplexSourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    /**
     * This constructor behaves like {@link #SingleThreadMultiplexSourceReaderBase(Supplier,
     * RecordEmitter, Configuration, SourceReaderContext)}, but accepts a specific {@link
     * FutureCompletingBlockingQueue}, {@link SingleThreadFetcherManager} and {@link
     * RecordEvaluator}.
     */
    public SingleThreadMultiplexSourceReaderBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                splitFetcherManager,
                recordEmitter,
                eofRecordEvaluator,
                config,
                context);
    }
}
