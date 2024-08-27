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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.util.Arrays;

/**
 * The {@link VariableLengthByteKeyAndValueComparator} is used by {@link KeyedSortPartitionOperator}
 * to compare records according to both the record key and record value. The length of record key
 * must be variable and will be read from {@link DataInputView}.
 */
@Internal
public class VariableLengthByteKeyAndValueComparator<INPUT>
        extends TypeComparator<Tuple2<byte[], INPUT>> {

    private final TypeComparator<INPUT> valueComparator;

    private byte[] keyReference;

    private INPUT valueReference;

    public VariableLengthByteKeyAndValueComparator(TypeComparator<INPUT> valueComparator) {
        this.valueComparator = valueComparator;
    }

    @Override
    public int hash(Tuple2<byte[], INPUT> record) {
        return record.hashCode();
    }

    @Override
    public void setReference(Tuple2<byte[], INPUT> toCompare) {
        this.keyReference = toCompare.f0;
        this.valueReference = toCompare.f1;
    }

    @Override
    public boolean equalToReference(Tuple2<byte[], INPUT> candidate) {
        return Arrays.equals(keyReference, candidate.f0) && valueReference.equals(candidate.f1);
    }

    @Override
    public int compareToReference(TypeComparator<Tuple2<byte[], INPUT>> referencedComparator) {
        byte[] otherKey =
                ((VariableLengthByteKeyAndValueComparator<INPUT>) referencedComparator)
                        .keyReference;
        INPUT otherValue =
                ((VariableLengthByteKeyAndValueComparator<INPUT>) referencedComparator)
                        .valueReference;
        int keyCmp = compareKey(otherKey, this.keyReference);
        if (keyCmp != 0) {
            return keyCmp;
        }
        return valueComparator.compare(this.valueReference, otherValue);
    }

    @Override
    public int compare(Tuple2<byte[], INPUT> first, Tuple2<byte[], INPUT> second) {
        int keyCmp = compareKey(first.f0, second.f0);
        if (keyCmp != 0) {
            return keyCmp;
        }
        return valueComparator.compare(first.f1, second.f1);
    }

    private int compareKey(byte[] first, byte[] second) {
        int firstLength = first.length;
        int secondLength = second.length;
        int minLength = Math.min(firstLength, secondLength);
        for (int i = 0; i < minLength; i++) {
            int cmp = Byte.compare(first[i], second[i]);

            if (cmp != 0) {
                return cmp;
            }
        }

        return Integer.compare(firstLength, secondLength);
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        int firstLength = firstSource.readInt();
        int secondLength = secondSource.readInt();
        int minLength = Math.min(firstLength, secondLength);
        while (minLength-- > 0) {
            byte firstValue = firstSource.readByte();
            byte secondValue = secondSource.readByte();

            int cmp = Byte.compare(firstValue, secondValue);
            if (cmp != 0) {
                return cmp;
            }
        }
        int lengthCompare = Integer.compare(firstLength, secondLength);
        if (lengthCompare != 0) {
            return lengthCompare;
        } else {
            return valueComparator.compareSerialized(firstSource, secondSource);
        }
    }

    @Override
    public boolean supportsNormalizedKey() {
        return false;
    }

    @Override
    public int getNormalizeKeyLen() {
        throw new UnsupportedOperationException(
                "Not supported as the data containing both key and value cannot generate normalized key.");
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return false;
    }

    @Override
    public void putNormalizedKey(
            Tuple2<byte[], INPUT> record, MemorySegment target, int offset, int numBytes) {
        throw new UnsupportedOperationException(
                "Not supported as the data containing both key and value cannot generate normalized key.");
    }

    @Override
    public boolean invertNormalizedKey() {
        return false;
    }

    @Override
    public TypeComparator<Tuple2<byte[], INPUT>> duplicate() {
        return new VariableLengthByteKeyAndValueComparator<>(this.valueComparator);
    }

    @Override
    public int extractKeys(Object record, Object[] target, int index) {
        target[index] = record;
        return 1;
    }

    @Override
    public TypeComparator<?>[] getFlatComparators() {
        return new TypeComparator[] {this};
    }

    @Override
    public boolean supportsSerializationWithKeyNormalization() {
        return false;
    }

    @Override
    public void writeWithKeyNormalization(Tuple2<byte[], INPUT> record, DataOutputView target) {
        throw new UnsupportedOperationException(
                "Not supported as the data containing both key and value cannot generate normalized key.");
    }

    @Override
    public Tuple2<byte[], INPUT> readWithKeyDenormalization(
            Tuple2<byte[], INPUT> reuse, DataInputView source) {
        throw new UnsupportedOperationException(
                "Not supported as the data containing both key and value cannot generate normalized key.");
    }
}
