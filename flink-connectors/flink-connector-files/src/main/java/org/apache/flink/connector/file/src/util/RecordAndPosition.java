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

/**
 * A record, together with the reader position to be stored in the checkpoint.
 *
 * <p>The position defines the point in the reader AFTER the record. Record processing and updating
 * checkpointed state happens atomically. The position points to where the reader should resume
 * after this record is processed.
 *
 * <p>For example, the very first record in a file split could have an {@code offset} of zero and a
 * {@code recordSkipCount} of one.
 *
 * <p>This class is immutable for safety. Use {@link MutableRecordAndPosition} if you need a mutable
 * version for efficiency.
 *
 * <p>Note on this design: Why do we not make the position point to the current record and always
 * skip one record after recovery (the just processed record)? We need to be able to support formats
 * where skipping records (even one) is not an option. For example formats that execute (pushed
 * down) filters may want to avoid a skip-record-count all together, so that they don't skip the
 * wrong records when the filter gets updated around a checkpoint/savepoint.
 */
@PublicEvolving
public class RecordAndPosition<E> {

    /**
     * Constant for the offset, reflecting that the position does not contain any offset
     * information. It is used in positions that are defined only by a number of records to skip.
     */
    public static final long NO_OFFSET = CheckpointedPosition.NO_OFFSET;

    // these are package private and non-final so we can mutate them from the
    // MutableRecordAndPosition
    E record;
    long offset;
    long recordSkipCount;

    /** Creates a new {@code RecordAndPosition} with the given record and position info. */
    public RecordAndPosition(E record, long offset, long recordSkipCount) {
        this.record = record;
        this.offset = offset;
        this.recordSkipCount = recordSkipCount;
    }

    /** Package private constructor for the {@link MutableRecordAndPosition}. */
    RecordAndPosition() {}

    // ------------------------------------------------------------------------

    public E getRecord() {
        return record;
    }

    public long getOffset() {
        return offset;
    }

    public long getRecordSkipCount() {
        return recordSkipCount;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("%s @ %d + %d", record, offset, recordSkipCount);
    }
}
