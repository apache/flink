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
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;
import org.apache.flink.types.NullValue;

import java.io.IOException;

/** Specialized comparator for NullValue based on CopyableValueComparator. */
@Internal
public class NullValueComparator extends TypeComparator<NullValue> {

    private static final long serialVersionUID = 1L;

    private final TypeComparator<?>[] comparators = new TypeComparator[] {this};

    private static final NullValueComparator INSTANCE = new NullValueComparator();

    public static NullValueComparator getInstance() {
        return INSTANCE;
    }

    private NullValueComparator() {}

    @Override
    public int hash(NullValue record) {
        return record.hashCode();
    }

    @Override
    public void setReference(NullValue toCompare) {}

    @Override
    public boolean equalToReference(NullValue candidate) {
        return true;
    }

    @Override
    public int compareToReference(TypeComparator<NullValue> referencedComparator) {
        return 0;
    }

    @Override
    public int compare(NullValue first, NullValue second) {
        return 0;
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        return 0;
    }

    @Override
    public boolean supportsNormalizedKey() {
        return NormalizableKey.class.isAssignableFrom(NullValue.class);
    }

    @Override
    public int getNormalizeKeyLen() {
        return NullValue.getInstance().getMaxNormalizedKeyLen();
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return keyBytes < getNormalizeKeyLen();
    }

    @Override
    public void putNormalizedKey(NullValue record, MemorySegment target, int offset, int numBytes) {
        record.copyNormalizedKey(target, offset, numBytes);
    }

    @Override
    public boolean invertNormalizedKey() {
        return false;
    }

    @Override
    public TypeComparator<NullValue> duplicate() {
        return NullValueComparator.getInstance();
    }

    @Override
    public int extractKeys(Object record, Object[] target, int index) {
        target[index] = record;
        return 1;
    }

    @Override
    public TypeComparator<?>[] getFlatComparators() {
        return comparators;
    }

    // --------------------------------------------------------------------------------------------
    // unsupported normalization
    // --------------------------------------------------------------------------------------------

    @Override
    public boolean supportsSerializationWithKeyNormalization() {
        return false;
    }

    @Override
    public void writeWithKeyNormalization(NullValue record, DataOutputView target)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NullValue readWithKeyDenormalization(NullValue reuse, DataInputView source)
            throws IOException {
        throw new UnsupportedOperationException();
    }
}
