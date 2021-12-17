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

package org.apache.flink.connector.file.src.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.reader.BulkFormat;

import javax.annotation.Nullable;

import java.util.Iterator;

/**
 * A simple {@link BulkFormat.RecordIterator} that returns the elements of an iterator, augmented
 * with position information.
 *
 * @param <E> The type of the record returned by the iterator.
 */
@PublicEvolving
public final class IteratorResultIterator<E> extends RecyclableIterator<E>
        implements BulkFormat.RecordIterator<E> {

    private final Iterator<E> records;

    private final MutableRecordAndPosition<E> recordAndPosition;

    /**
     * Creates a new {@code RecordIterator} returning the records from the given iterator, augmented
     * with their position information.
     *
     * <p>Each record's {@link RecordAndPosition} will have the same offset value for {@link
     * RecordAndPosition#getOffset()}. The first returned record will have a records-to-skip count
     * of {@code startingSkipCount + 1}, following the contract that each record needs to point to
     * the position AFTER itself (because a checkpoint taken after the record was emitted needs to
     * resume from after that record).
     */
    public IteratorResultIterator(
            final Iterator<E> records, final long offset, final long startingSkipCount) {
        this(records, offset, startingSkipCount, null);
    }

    /**
     * Creates a new {@code RecordIterator} returning the records from the given iterator, augmented
     * with their position information. When the iterator is marked as done (via {@link
     * #releaseBatch()}, the given {@code recycler} is called.
     *
     * <p>Each record's {@link RecordAndPosition} will have the same offset value for {@link
     * RecordAndPosition#getOffset()}. The first returned record will have a records-to-skip count
     * of {@code startingSkipCount + 1}, following the contract that each record needs to point to
     * the position AFTER itself (because a checkpoint taken after the record was emitted needs to
     * resume from after that record).
     */
    public IteratorResultIterator(
            final Iterator<E> records,
            final long offset,
            final long startingSkipCount,
            final @Nullable Runnable recycler) {

        super(recycler);
        this.records = records;
        this.recordAndPosition = new MutableRecordAndPosition<>();
        this.recordAndPosition.setPosition(offset, startingSkipCount);
    }

    // -------------------------------------------------------------------------
    //  Result Iterator Methods
    // -------------------------------------------------------------------------

    @Nullable
    @Override
    public RecordAndPosition<E> next() {
        if (records.hasNext()) {
            recordAndPosition.setNext(records.next());
            return recordAndPosition;
        } else {
            return null;
        }
    }
}
