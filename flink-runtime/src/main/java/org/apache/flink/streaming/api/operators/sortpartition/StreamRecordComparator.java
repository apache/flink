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

package org.apache.flink.streaming.api.operators.sortpartition;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;

/** Comparator that delegates sorting to the wrapped StreamRecord value comparator. */
final class StreamRecordComparator<T> extends TypeComparator<StreamRecord<T>> {

    private static final long serialVersionUID = 1L;

    private final TypeComparator<T> valueComparator;
    private final TypeSerializer<StreamRecord<T>> streamRecordSerializer;

    StreamRecordComparator(
            TypeComparator<T> valueComparator,
            TypeSerializer<StreamRecord<T>> streamRecordSerializer) {
        this.valueComparator = valueComparator;
        this.streamRecordSerializer = streamRecordSerializer;
    }

    @Override
    public int hash(StreamRecord<T> record) {
        return valueComparator.hash(record.getValue());
    }

    @Override
    public void setReference(StreamRecord<T> toCompare) {
        valueComparator.setReference(toCompare.getValue());
    }

    @Override
    public boolean equalToReference(StreamRecord<T> candidate) {
        return valueComparator.equalToReference(candidate.getValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareToReference(TypeComparator<StreamRecord<T>> referencedComparator) {
        return valueComparator.compareToReference(
                ((StreamRecordComparator<T>) referencedComparator).valueComparator);
    }

    @Override
    public boolean supportsCompareAgainstReference() {
        return valueComparator.supportsCompareAgainstReference();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compareAgainstReference(Comparable[] keys) {
        return valueComparator.compareAgainstReference(keys);
    }

    @Override
    public int compare(StreamRecord<T> first, StreamRecord<T> second) {
        return valueComparator.compare(first.getValue(), second.getValue());
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        StreamRecord<T> firstRecord = streamRecordSerializer.deserialize(firstSource);
        StreamRecord<T> secondRecord = streamRecordSerializer.deserialize(secondSource);
        return compare(firstRecord, secondRecord);
    }

    @Override
    public boolean supportsNormalizedKey() {
        return valueComparator.supportsNormalizedKey();
    }

    @Override
    public boolean supportsSerializationWithKeyNormalization() {
        return false;
    }

    @Override
    public int getNormalizeKeyLen() {
        return valueComparator.getNormalizeKeyLen();
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return valueComparator.isNormalizedKeyPrefixOnly(keyBytes);
    }

    @Override
    public void putNormalizedKey(
            StreamRecord<T> record, MemorySegment target, int offset, int numBytes) {
        valueComparator.putNormalizedKey(record.getValue(), target, offset, numBytes);
    }

    @Override
    public void writeWithKeyNormalization(StreamRecord<T> record, DataOutputView target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamRecord<T> readWithKeyDenormalization(StreamRecord<T> reuse, DataInputView source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean invertNormalizedKey() {
        return valueComparator.invertNormalizedKey();
    }

    @Override
    public TypeComparator<StreamRecord<T>> duplicate() {
        return new StreamRecordComparator<>(
                valueComparator.duplicate(), streamRecordSerializer.duplicate());
    }

    @Override
    public int extractKeys(Object record, Object[] target, int index) {
        return valueComparator.extractKeys(((StreamRecord<?>) record).getValue(), target, index);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public TypeComparator[] getFlatComparators() {
        return valueComparator.getFlatComparators();
    }
}
