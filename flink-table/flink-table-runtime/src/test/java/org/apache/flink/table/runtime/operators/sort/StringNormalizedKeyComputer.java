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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;

/** Example String {@link NormalizedKeyComputer}. */
public class StringNormalizedKeyComputer implements NormalizedKeyComputer {

    @Override
    public void putKey(RowData record, MemorySegment target, int offset) {
        if (record.isNullAt(0)) {
            SortUtil.minNormalizedKey(target, offset, 8);
        } else {
            SortUtil.putStringNormalizedKey(
                    (BinaryStringData) record.getString(0), target, offset, 8);
        }
        target.putLong(offset, Long.reverseBytes(target.getLong(offset)));
    }

    @Override
    public int compareKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {
        long l0 = segI.getLong(offsetI);
        long l1 = segJ.getLong(offsetJ);
        if (l0 != l1) {
            return ((l0 < l1) ^ (l0 < 0) ^ (l1 < 0) ? -1 : 1);
        }
        return 0;
    }

    @Override
    public void swapKey(MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {
        long temp0 = segI.getLong(offsetI);
        segI.putLong(offsetI, segJ.getLong(offsetJ));
        segJ.putLong(offsetJ, temp0);
    }

    @Override
    public int getNumKeyBytes() {
        return 8;
    }

    @Override
    public boolean isKeyFullyDetermines() {
        return false;
    }

    @Override
    public boolean invertKey() {
        return false;
    }
}
