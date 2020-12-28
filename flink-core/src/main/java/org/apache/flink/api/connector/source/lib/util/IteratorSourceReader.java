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

package org.apache.flink.api.connector.source.lib.util;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link SourceReader} that returns the values of an iterator, supplied via an {@link
 * IteratorSourceSplit}.
 *
 * <p>The {@code IteratorSourceSplit} is also responsible for taking the current iterator and
 * turning it back into a split for checkpointing.
 *
 * @param <E> The type of events returned by the reader.
 * @param <IterT> The type of the iterator that produces the events. This type exists to make the
 *     conversion between iterator and {@code IteratorSourceSplit} type safe.
 * @param <SplitT> The concrete type of the {@code IteratorSourceSplit} that creates and converts
 *     the iterator that produces this reader's elements.
 */
public class IteratorSourceReader<
                E, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
        implements SourceReader<E, SplitT> {

    /** The context for this reader, to communicate with the enumerator. */
    private final SourceReaderContext context;

    /** The availability future. This reader is available as soon as a split is assigned. */
    private final CompletableFuture<Void> availability;

    /**
     * The iterator producing data. Non-null after a split has been assigned. This field is null or
     * non-null always together with the {@link #currentSplit} field.
     */
    @Nullable private IterT iterator;

    /**
     * The split whose data we return. Non-null after a split has been assigned. This field is null
     * or non-null always together with the {@link #iterator} field.
     */
    @Nullable private SplitT currentSplit;

    /** The remaining splits. Null means no splits have yet been assigned. */
    @Nullable private Queue<SplitT> remainingSplits;

    public IteratorSourceReader(SourceReaderContext context) {
        this.context = checkNotNull(context);
        this.availability = new CompletableFuture<>();
    }

    // ------------------------------------------------------------------------

    @Override
    public void start() {
        // request a split only if we did not get one during restore
        if (iterator == null) {
            context.sendSplitRequest();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<E> output) {
        if (iterator != null && iterator.hasNext()) {
            output.collect(iterator.next());
            return InputStatus.MORE_AVAILABLE;
        } else if (remainingSplits == null) {
            // nothing assigned yet, need to wait and come back when splits have been assigned
            return InputStatus.NOTHING_AVAILABLE;
        } else {
            currentSplit = remainingSplits.poll();
            if (currentSplit != null) {
                iterator = currentSplit.getIterator();
                return pollNext(output);
            } else {
                return InputStatus.END_OF_INPUT;
            }
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void addSplits(List<SplitT> splits) {
        checkState(remainingSplits == null, "Cannot accept more than one split assignment");
        remainingSplits = new ArrayDeque<>(splits);
        availability.complete(null); // from now on we are always available
    }

    @Override
    public void notifyNoMoreSplits() {
        // if we get this after we already had a split, we must have requested more than
        // one split, which is not expected here.
        checkState(remainingSplits == null, "Unexpected response, requested more than one split.");

        // non-null queue signals splits were assigned, in this case no splits
        remainingSplits = new ArrayDeque<>();
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        if (remainingSplits == null) {
            // no assignment yet
            return Collections.emptyList();
        }
        final ArrayList<SplitT> allSplits = new ArrayList<>(1 + remainingSplits.size());
        if (iterator != null && iterator.hasNext()) {
            @SuppressWarnings("unchecked")
            final SplitT inProgressSplit =
                    (SplitT) currentSplit.getUpdatedSplitForIterator(iterator);
            allSplits.add(inProgressSplit);
        }
        allSplits.addAll(remainingSplits);
        return allSplits;
    }

    @Override
    public void close() throws Exception {}
}
