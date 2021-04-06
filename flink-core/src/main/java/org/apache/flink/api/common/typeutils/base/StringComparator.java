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
import org.apache.flink.types.StringValue;

import java.io.IOException;

@Internal
public final class StringComparator extends BasicTypeComparator<String> {

    private static final long serialVersionUID = 1L;

    private static final int HIGH_BIT = 0x1 << 7;

    private static final int HIGH_BIT2 = 0x1 << 13;

    private static final int HIGH_BIT2_MASK = 0x3 << 6;

    public StringComparator(boolean ascending) {
        super(ascending);
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        String s1 = StringValue.readString(firstSource);
        String s2 = StringValue.readString(secondSource);
        int comp = s1.compareTo(s2);
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public boolean supportsNormalizedKey() {
        return true;
    }

    @Override
    public boolean supportsSerializationWithKeyNormalization() {
        return false;
    }

    @Override
    public int getNormalizeKeyLen() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return true;
    }

    @Override
    public void putNormalizedKey(String record, MemorySegment target, int offset, int len) {
        final int limit = offset + len;
        final int end = record.length();
        int pos = 0;

        while (pos < end && offset < limit) {
            char c = record.charAt(pos++);
            if (c < HIGH_BIT) {
                target.put(offset++, (byte) c);
            } else if (c < HIGH_BIT2) {
                target.put(offset++, (byte) ((c >>> 7) | HIGH_BIT));
                if (offset < limit) {
                    target.put(offset++, (byte) c);
                }
            } else {
                target.put(offset++, (byte) ((c >>> 10) | HIGH_BIT2_MASK));
                if (offset < limit) {
                    target.put(offset++, (byte) (c >>> 2));
                }
                if (offset < limit) {
                    target.put(offset++, (byte) c);
                }
            }
        }
        while (offset < limit) {
            target.put(offset++, (byte) 0);
        }
    }

    @Override
    public StringComparator duplicate() {
        return new StringComparator(ascendingComparison);
    }
}
