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

/**
 * A mutable version of the {@link RecordAndPosition}.
 *
 * <p>This mutable object is useful in cases where only once instance of a {@code RecordAndPosition}
 * is needed at a time, like for the result values of the {@link BulkFormat.RecordIterator}.
 */
@PublicEvolving
public class MutableRecordAndPosition<E> extends RecordAndPosition<E> {

    /** Updates the record and position in this object. */
    public void set(E record, long offset, long recordSkipCount) {
        this.record = record;
        this.offset = offset;
        this.recordSkipCount = recordSkipCount;
    }

    /** Sets the position without setting a record. */
    public void setPosition(long offset, long recordSkipCount) {
        this.offset = offset;
        this.recordSkipCount = recordSkipCount;
    }

    /** Sets the next record of a sequence. This increments the {@code recordSkipCount} by one. */
    public void setNext(E record) {
        this.record = record;
        this.recordSkipCount++;
    }
}
