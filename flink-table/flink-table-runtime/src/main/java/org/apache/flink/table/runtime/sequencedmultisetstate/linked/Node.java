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

package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.table.data.RowData;

import java.util.Objects;

class Node {
    final RowData row;
    private final long sqn;
    final Long prevSqn;
    final Long nextSqn;
    final Long nextSqnForRecord;
    final Long timestamp; // for future TTL support

    Node(RowData row, long sqn, Long prevSqn, Long nextSqn, Long nextSqnForRecord, Long timestamp) {
        this.row = row;
        this.sqn = sqn;
        this.prevSqn = prevSqn;
        this.nextSqn = nextSqn;
        this.nextSqnForRecord = nextSqnForRecord;
        this.timestamp = timestamp;
    }

    public boolean isLastForRecord() {
        return nextSqnForRecord == null;
    }

    public boolean isLowestSqn() {
        return !hasPrev();
    }

    public boolean isHighestSqn() {
        return !hasNext();
    }

    public boolean hasPrev() {
        return prevSqn != null;
    }

    public boolean hasNext() {
        return nextSqn != null;
    }

    public Node withNextForRecord(Long nextSeqNoForRecord) {
        return new Node(row, sqn, prevSqn, nextSqn, nextSeqNoForRecord, timestamp);
    }

    public Node withNext(Long nextSeqNo) {
        return new Node(row, sqn, prevSqn, nextSeqNo, nextSqnForRecord, timestamp);
    }

    public Node withPrev(Long prevSeqNo) {
        return new Node(row, sqn, prevSeqNo, nextSqn, nextSqnForRecord, timestamp);
    }

    public Node withRow(RowData row, long timestamp) {
        return new Node(row, sqn, prevSqn, nextSqn, nextSqnForRecord, timestamp);
    }

    public RowData getRow() {
        return row;
    }

    public long getSqn() {
        return sqn;
    }

    public Long getPrevSqn() {
        return prevSqn;
    }

    public Long getNextSqn() {
        return nextSqn;
    }

    public Long getNextSqnForRecord() {
        return nextSqnForRecord;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Node)) {
            return false;
        }
        Node node = (Node) o;
        // do not compare row data since:
        // 1. the type might be different after deserialization, e.g. GenericRowData vs
        // BinaryRowData
        // 2. proper comparison requires (generated) equalizer
        // 3. equals is only used in tests (as opposed to RowDataKey)
        return sqn == node.sqn
                && Objects.equals(prevSqn, node.prevSqn)
                && Objects.equals(nextSqn, node.nextSqn)
                && Objects.equals(nextSqnForRecord, node.nextSqnForRecord);
    }

    @Override
    public int hashCode() {
        // rowData is ignored - see equals
        return Objects.hash(sqn, prevSqn, nextSqn, nextSqnForRecord);
    }
}
