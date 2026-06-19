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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.types.RowKind;

import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class wraps keys of type {@link RowData} for the following purposes:
 *
 * <ol>
 *   <li>Fix the {@link RowKind} to be the same in all keys.
 *   <li>Project the fields in case of upsert key.
 *   <li>Fix {@link Object#equals(Object)} and hashCode for heap state backend.
 *   <li>Potentially fix mutability for heap state backend (by copying using serializer)
 * </ol>
 */
@Internal
class RowDataKey {
    private final RecordEqualiser equaliser;
    private final HashFunction hashFunction;
    final RowData rowData;

    RowDataKey(RecordEqualiser equaliser, HashFunction hashFunction) {
        this.equaliser = checkNotNull(equaliser);
        this.hashFunction = checkNotNull(hashFunction);
        this.rowData = null;
    }

    public RowDataKey(RowData rowData, RecordEqualiser equaliser, HashFunction hashFunction) {
        this.equaliser = checkNotNull(equaliser);
        this.hashFunction = checkNotNull(hashFunction);
        this.rowData = checkNotNull(rowData);
    }

    public static RowDataKey toKeyNotProjected(
            RowData row, RecordEqualiser equaliser, HashFunction hasher) {
        return toKey(row, equaliser, hasher);
    }

    public static RowDataKey toKey(RowData row, RecordEqualiser equaliser, HashFunction hasher) {
        row.setRowKind(INSERT);
        return new RowDataKey(row, equaliser, hasher);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RowDataKey)) {
            return false;
        }
        RowDataKey other = (RowDataKey) o;
        return equaliser.equals(rowData, other.rowData);
    }

    @Override
    public int hashCode() {
        return hashFunction.hashCode(rowData);
    }
}
