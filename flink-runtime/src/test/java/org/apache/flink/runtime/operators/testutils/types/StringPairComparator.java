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

package org.apache.flink.runtime.operators.testutils.types;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.StringValue;

import java.io.IOException;

@SuppressWarnings("rawtypes")
public class StringPairComparator extends TypeComparator<StringPair> {

    private static final long serialVersionUID = 1L;

    private String reference;

    private final TypeComparator[] comparators = new TypeComparator[] {new StringComparator(true)};

    @Override
    public int hash(StringPair record) {
        return record.getKey().hashCode();
    }

    @Override
    public void setReference(StringPair toCompare) {
        this.reference = toCompare.getKey();
    }

    @Override
    public boolean equalToReference(StringPair candidate) {
        return this.reference.equals(candidate.getKey());
    }

    @Override
    public int compareToReference(TypeComparator<StringPair> referencedComparator) {
        return this.reference.compareTo(((StringPairComparator) referencedComparator).reference);
    }

    @Override
    public int compare(StringPair first, StringPair second) {
        return first.getKey().compareTo(second.getKey());
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        return StringValue.readString(firstSource).compareTo(StringValue.readString(secondSource));
    }

    @Override
    public boolean supportsNormalizedKey() {
        return false;
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
        return false;
    }

    @Override
    public void putNormalizedKey(
            StringPair record, MemorySegment target, int offset, int numBytes) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void writeWithKeyNormalization(StringPair record, DataOutputView target)
            throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public StringPair readWithKeyDenormalization(StringPair record, DataInputView source)
            throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean invertNormalizedKey() {
        return false;
    }

    @Override
    public TypeComparator<StringPair> duplicate() {
        return new StringPairComparator();
    }

    @Override
    public int extractKeys(Object record, Object[] target, int index) {
        target[index] = ((StringPair) record).getKey();
        return 1;
    }

    @Override
    public TypeComparator[] getFlatComparators() {
        return comparators;
    }
}
