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

/**
 * A simple {@link BulkFormat.RecordIterator} that returns a single value. The iterator is mutable
 * to support object reuse and supports recycling.
 *
 * @param <E> The type of the record returned by the iterator.
 */
@PublicEvolving
public final class SingletonResultIterator<E> extends RecyclableIterator<E>
        implements BulkFormat.RecordIterator<E> {

    @Nullable private RecordAndPosition<E> element;

    private final MutableRecordAndPosition<E> recordAndPosition;

    public SingletonResultIterator() {
        this(null);
    }

    public SingletonResultIterator(@Nullable Runnable recycler) {
        super(recycler);
        this.recordAndPosition = new MutableRecordAndPosition<>();
    }

    // -------------------------------------------------------------------------
    //  Setting
    // -------------------------------------------------------------------------

    /**
     * Sets the single record to be returned by this iterator. The offset and records-to-skip count
     * will be used as provided here for the returned {@link RecordAndPosition}, meaning they need
     * to point to AFTER this specific record (because a checkpoint taken after the record was
     * processed needs to resume from after this record).
     */
    public void set(final E element, final long offset, final long skipCount) {
        this.recordAndPosition.set(element, offset, skipCount);
        this.element = this.recordAndPosition;
    }

    // -------------------------------------------------------------------------
    //  Result Iterator Methods
    // -------------------------------------------------------------------------

    @Nullable
    @Override
    public RecordAndPosition<E> next() {
        final RecordAndPosition<E> next = this.element;
        this.element = null;
        return next;
    }
}
