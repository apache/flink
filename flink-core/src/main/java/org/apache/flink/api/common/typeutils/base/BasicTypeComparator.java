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

import java.io.IOException;

@Internal
public abstract class BasicTypeComparator<T extends Comparable<T>> extends TypeComparator<T>
        implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private transient T reference;

    protected final boolean ascendingComparison;

    // For use by getComparators
    @SuppressWarnings("rawtypes")
    private final TypeComparator[] comparators = new TypeComparator[] {this};

    protected BasicTypeComparator(boolean ascending) {
        this.ascendingComparison = ascending;
    }

    @Override
    public int hash(T value) {
        return value.hashCode();
    }

    @Override
    public void setReference(T toCompare) {
        this.reference = toCompare;
    }

    @Override
    public boolean equalToReference(T candidate) {
        return candidate.equals(reference);
    }

    @Override
    public int compareToReference(TypeComparator<T> referencedComparator) {
        int comp = ((BasicTypeComparator<T>) referencedComparator).reference.compareTo(reference);
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public int compare(T first, T second) {
        int cmp = first.compareTo(second);
        return ascendingComparison ? cmp : -cmp;
    }

    @Override
    public boolean invertNormalizedKey() {
        return !ascendingComparison;
    }

    @Override
    public boolean supportsSerializationWithKeyNormalization() {
        return false;
    }

    @Override
    public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException();
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

    @Override
    public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
        throw new UnsupportedOperationException();
    }
}
