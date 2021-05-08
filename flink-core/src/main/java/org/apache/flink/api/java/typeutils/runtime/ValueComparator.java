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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;

import com.esotericsoftware.kryo.Kryo;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;

/** Comparator for all Value types that extend Key */
@Internal
public class ValueComparator<T extends Value & Comparable<T>> extends TypeComparator<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> type;

    private final boolean ascendingComparison;

    private transient T reference;

    private transient T tempReference;

    private transient Kryo kryo;

    @SuppressWarnings("rawtypes")
    private final TypeComparator[] comparators = new TypeComparator[] {this};

    public ValueComparator(boolean ascending, Class<T> type) {
        this.type = type;
        this.ascendingComparison = ascending;
    }

    @Override
    public int hash(T record) {
        return record.hashCode();
    }

    @Override
    public void setReference(T toCompare) {
        checkKryoInitialized();

        reference = KryoUtils.copy(toCompare, kryo, new ValueSerializer<T>(type));
    }

    @Override
    public boolean equalToReference(T candidate) {
        return candidate.equals(this.reference);
    }

    @Override
    public int compareToReference(TypeComparator<T> referencedComparator) {
        T otherRef = ((ValueComparator<T>) referencedComparator).reference;
        int comp = otherRef.compareTo(reference);
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public int compare(T first, T second) {
        int comp = first.compareTo(second);
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        if (reference == null) {
            reference = InstantiationUtil.instantiate(type, Value.class);
        }
        if (tempReference == null) {
            tempReference = InstantiationUtil.instantiate(type, Value.class);
        }

        reference.read(firstSource);
        tempReference.read(secondSource);
        int comp = reference.compareTo(tempReference);
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public boolean supportsNormalizedKey() {
        return NormalizableKey.class.isAssignableFrom(type);
    }

    @Override
    public int getNormalizeKeyLen() {
        if (reference == null) {
            reference = InstantiationUtil.instantiate(type, Value.class);
        }

        NormalizableKey<?> key = (NormalizableKey<?>) reference;
        return key.getMaxNormalizedKeyLen();
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return keyBytes < getNormalizeKeyLen();
    }

    @Override
    public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {
        NormalizableKey<?> key = (NormalizableKey<?>) record;
        key.copyNormalizedKey(target, offset, numBytes);
    }

    @Override
    public boolean invertNormalizedKey() {
        return !ascendingComparison;
    }

    @Override
    public TypeComparator<T> duplicate() {
        return new ValueComparator<T>(ascendingComparison, type);
    }

    private void checkKryoInitialized() {
        if (this.kryo == null) {
            this.kryo = new Kryo();

            Kryo.DefaultInstantiatorStrategy instantiatorStrategy =
                    new Kryo.DefaultInstantiatorStrategy();
            instantiatorStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            kryo.setInstantiatorStrategy(instantiatorStrategy);

            this.kryo.setAsmEnabled(true);
            this.kryo.register(type);
        }
    }

    @Override
    public int extractKeys(Object record, Object[] target, int index) {
        target[index] = record;
        return 1;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public TypeComparator[] getFlatComparators() {
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
    public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
        throw new UnsupportedOperationException();
    }
}
