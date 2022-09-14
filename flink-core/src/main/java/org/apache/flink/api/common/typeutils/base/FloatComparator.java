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
public final class FloatComparator extends BasicTypeComparator<Float> {

    private static final long serialVersionUID = 1L;

    public FloatComparator(boolean ascending) {
        super(ascending);
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        float l1 = firstSource.readFloat();
        float l2 = secondSource.readFloat();
        int comp = (l1 < l2 ? -1 : (l1 > l2 ? 1 : 0));
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public boolean supportsNormalizedKey() {
        return false;
    }

    @Override
    public int getNormalizeKeyLen() {
        return 0;
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return true;
    }

    @Override
    public void putNormalizedKey(Float value, MemorySegment target, int offset, int numBytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FloatComparator duplicate() {
        return new FloatComparator(ascendingComparison);
    }
}
