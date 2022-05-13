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

@Internal
public final class EnumComparator<T extends Enum<T>> extends BasicTypeComparator<T> {

    private static final long serialVersionUID = 1L;

    public EnumComparator(boolean ascending) {
        super(ascending);
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        int i1 = firstSource.readInt();
        int i2 = secondSource.readInt();
        int comp = (i1 < i2 ? -1 : (i1 == i2 ? 0 : 1));
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public boolean supportsNormalizedKey() {
        return true;
    }

    @Override
    public int getNormalizeKeyLen() {
        return 4;
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return keyBytes < 4;
    }

    @Override
    public void putNormalizedKey(T iValue, MemorySegment target, int offset, int numBytes) {
        int value = iValue.ordinal() - Integer.MIN_VALUE;

        // see IntValue for an explanation of the logic
        if (numBytes == 4) {
            // default case, full normalized key
            target.putIntBigEndian(offset, value);
        } else if (numBytes <= 0) {
        } else if (numBytes < 4) {
            for (int i = 0; numBytes > 0; numBytes--, i++) {
                target.put(offset + i, (byte) (value >>> ((3 - i) << 3)));
            }
        } else {
            target.putLongBigEndian(offset, value);
            for (int i = 4; i < numBytes; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }

    @Override
    public EnumComparator<T> duplicate() {
        return new EnumComparator<T>(ascendingComparison);
    }
}
