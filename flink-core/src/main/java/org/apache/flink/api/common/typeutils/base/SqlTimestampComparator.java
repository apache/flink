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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.sql.Timestamp;

/** Comparator for comparing Java SQL Timestamps. */
@Internal
public final class SqlTimestampComparator extends BasicTypeComparator<java.util.Date> {

    private static final long serialVersionUID = 1L;

    public SqlTimestampComparator(boolean ascending) {
        super(ascending);
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        // compare Date part
        final int comp =
                DateComparator.compareSerializedDate(
                        firstSource, secondSource, ascendingComparison);
        // compare nanos
        if (comp == 0) {
            final int i1 = firstSource.readInt();
            final int i2 = secondSource.readInt();
            final int comp2 = (i1 < i2 ? -1 : (i1 == i2 ? 0 : 1));
            return ascendingComparison ? comp2 : -comp2;
        }
        return comp;
    }

    @Override
    public boolean supportsNormalizedKey() {
        return true;
    }

    @Override
    public int getNormalizeKeyLen() {
        return 12;
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return keyBytes < 12;
    }

    @Override
    public void putNormalizedKey(
            java.util.Date record, MemorySegment target, int offset, int numBytes) {
        // put Date key
        DateComparator.putNormalizedKeyDate(record, target, offset, numBytes > 8 ? 8 : numBytes);
        numBytes -= 8;
        offset += 8;
        if (numBytes <= 0) {
            // nothing to do
        }
        // put nanos
        else if (numBytes < 4) {
            final int nanos = ((Timestamp) record).getNanos();
            for (int i = 0; numBytes > 0; numBytes--, i++) {
                target.put(offset + i, (byte) (nanos >>> ((3 - i) << 3)));
            }
        }
        // put nanos with padding
        else {
            final int nanos = ((Timestamp) record).getNanos();
            target.putIntBigEndian(offset, nanos);
            for (int i = 4; i < numBytes; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }

    @Override
    public SqlTimestampComparator duplicate() {
        return new SqlTimestampComparator(ascendingComparison);
    }
}
