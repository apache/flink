/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state.changelog;

import org.apache.flink.runtime.state.changelog.SequenceNumber.GenericSequenceNumber;
import org.apache.flink.util.Preconditions;

public interface SequenceNumberRange {
    /** Inclusive. */
    SequenceNumber from();
    /** Exclusive. */
    SequenceNumber to();

    /** @return the size of this range (positive) or zero if it is empty */
    long size();

    /**
     * @return true if {@link #from} &le; sqn &lt; {@link #to} (this implies that the range is not
     *     empty, i.e. to &gt; from)
     */
    boolean contains(SequenceNumber sqn);

    /** @return true if {@link #from} &ge; {@link #to}, false otherwise. */
    boolean isEmpty();

    /**
     * @param from inclusive
     * @param to exclusive
     */
    static SequenceNumberRange generic(SequenceNumber from, SequenceNumber to) {
        return new GenericSequenceNumberRange(
                (GenericSequenceNumber) from, (GenericSequenceNumber) to);
    }

    class GenericSequenceNumberRange implements SequenceNumberRange {
        private final GenericSequenceNumber from;
        private final GenericSequenceNumber to;
        private final long size;

        public GenericSequenceNumberRange(GenericSequenceNumber from, GenericSequenceNumber to) {
            this.from = from;
            this.to = to;
            this.size = to.number - from.number;
            Preconditions.checkArgument(size >= 0);
        }

        @Override
        public SequenceNumber from() {
            return from;
        }

        @Override
        public SequenceNumber to() {
            return to;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public boolean contains(SequenceNumber sqn) {
            return from.compareTo(sqn) <= 0 && sqn.compareTo(to) < 0;
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public String toString() {
            return String.format("from=%s, to=%s, size=%d", from, to, size);
        }
    }
}
