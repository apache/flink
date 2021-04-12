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
import java.time.LocalTime;

@Internal
public final class LocalTimeComparator extends BasicTypeComparator<LocalTime> {

    private static final long serialVersionUID = 1L;

    public LocalTimeComparator(boolean ascending) {
        super(ascending);
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        return compareSerializedLocalTime(firstSource, secondSource, ascendingComparison);
    }

    @Override
    public boolean supportsNormalizedKey() {
        return true;
    }

    @Override
    public int getNormalizeKeyLen() {
        return 7;
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return keyBytes < getNormalizeKeyLen();
    }

    @Override
    public void putNormalizedKey(LocalTime record, MemorySegment target, int offset, int numBytes) {
        putNormalizedKeyLocalTime(record, target, offset, numBytes);
    }

    @Override
    public LocalTimeComparator duplicate() {
        return new LocalTimeComparator(ascendingComparison);
    }

    // --------------------------------------------------------------------------------------------
    //                           Static Helpers for Date Comparison
    // --------------------------------------------------------------------------------------------

    public static int compareSerializedLocalTime(
            DataInputView firstSource, DataInputView secondSource, boolean ascendingComparison)
            throws IOException {
        int cmp = firstSource.readByte() - secondSource.readByte();
        if (cmp == 0) {
            cmp = firstSource.readByte() - secondSource.readByte();
            if (cmp == 0) {
                cmp = firstSource.readByte() - secondSource.readByte();
                if (cmp == 0) {
                    cmp = firstSource.readInt() - secondSource.readInt();
                }
            }
        }
        return ascendingComparison ? cmp : -cmp;
    }

    public static void putNormalizedKeyLocalTime(
            LocalTime record, MemorySegment target, int offset, int numBytes) {
        int hour = record.getHour();
        if (numBytes > 0) {
            target.put(offset, (byte) (hour & 0xff - Byte.MIN_VALUE));
            numBytes -= 1;
            offset += 1;
        }

        int minute = record.getMinute();
        if (numBytes > 0) {
            target.put(offset, (byte) (minute & 0xff - Byte.MIN_VALUE));
            numBytes -= 1;
            offset += 1;
        }

        int second = record.getSecond();
        if (numBytes > 0) {
            target.put(offset, (byte) (second & 0xff - Byte.MIN_VALUE));
            numBytes -= 1;
            offset += 1;
        }

        int nano = record.getNano();
        int unsignedNano = nano - Integer.MIN_VALUE;
        if (numBytes >= 4) {
            target.putIntBigEndian(offset, unsignedNano);
            numBytes -= 4;
            offset += 4;
        } else if (numBytes > 0) {
            for (int i = 0; numBytes > 0; numBytes--, i++) {
                target.put(offset + i, (byte) (unsignedNano >>> ((3 - i) << 3)));
            }
            return;
        }

        for (int i = 0; i < numBytes; i++) {
            target.put(offset + i, (byte) 0);
        }
    }
}
