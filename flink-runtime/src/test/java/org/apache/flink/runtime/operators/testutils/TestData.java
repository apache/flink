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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/** Test data utilities classes. */
public final class TestData {

    /** Private constructor (container class should not be instantiated) */
    private TestData() {}

    /** Tuple2<Integer, String> generator. */
    public static class TupleGenerator implements MutableObjectIterator<Tuple2<Integer, String>> {

        public enum KeyMode {
            SORTED,
            RANDOM,
            SORTED_SPARSE
        };

        public enum ValueMode {
            FIX_LENGTH,
            RANDOM_LENGTH,
            CONSTANT
        };

        private static char[] alpha = {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'a', 'b', 'c', 'd',
            'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm'
        };

        private final long seed;

        private final int keyMax;

        private final float keyDensity;

        private final int valueLength;

        private final KeyMode keyMode;

        private final ValueMode valueMode;

        private Random random;

        private int counter;

        private int key;
        private String value;

        public TupleGenerator(long seed, int keyMax, int valueLength) {
            this(seed, keyMax, valueLength, KeyMode.RANDOM, ValueMode.FIX_LENGTH);
        }

        public TupleGenerator(
                long seed, int keyMax, int valueLength, KeyMode keyMode, ValueMode valueMode) {
            this(seed, keyMax, valueLength, keyMode, valueMode, null);
        }

        public TupleGenerator(
                long seed,
                int keyMax,
                int valueLength,
                KeyMode keyMode,
                ValueMode valueMode,
                String constant) {
            this(seed, keyMax, 1.0f, valueLength, keyMode, valueMode, constant);
        }

        public TupleGenerator(
                long seed,
                int keyMax,
                float keyDensity,
                int valueLength,
                KeyMode keyMode,
                ValueMode valueMode,
                String constant) {
            this.seed = seed;
            this.keyMax = keyMax;
            this.keyDensity = keyDensity;
            this.valueLength = valueLength;
            this.keyMode = keyMode;
            this.valueMode = valueMode;

            this.random = new Random(seed);
            this.counter = 0;

            this.value = constant == null ? null : constant;
        }

        public Tuple2<Integer, String> next(Tuple2<Integer, String> reuse) {
            this.key = nextKey();
            if (this.valueMode != ValueMode.CONSTANT) {
                this.value = randomString();
            }
            reuse.setFields(this.key, this.value);
            return reuse;
        }

        public Tuple2<Integer, String> next() {
            return next(new Tuple2<Integer, String>());
        }

        public boolean next(org.apache.flink.types.Value[] target) {
            this.key = nextKey();
            // TODO change this to something proper
            ((IntValue) target[0]).setValue(this.key);
            ((IntValue) target[1]).setValue(random.nextInt());
            return true;
        }

        private int nextKey() {
            if (keyMode == KeyMode.SORTED) {
                return ++counter;
            } else if (keyMode == KeyMode.SORTED_SPARSE) {
                int max = (int) (1 / keyDensity);
                counter += random.nextInt(max) + 1;
                return counter;
            } else {
                return Math.abs(random.nextInt() % keyMax) + 1;
            }
        }

        public void reset() {
            this.random = new Random(seed);
            this.counter = 0;
        }

        private String randomString() {
            int length;

            if (valueMode == ValueMode.FIX_LENGTH) {
                length = valueLength;
            } else {
                length = valueLength - random.nextInt(valueLength / 3);
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                sb.append(alpha[random.nextInt(alpha.length)]);
            }
            return sb.toString();
        }
    }

    /** Tuple reader mock. */
    public static class TupleGeneratorIterator
            implements MutableObjectIterator<Tuple2<Integer, String>> {

        private final TupleGenerator generator;

        private final int numberOfRecords;

        private int counter;

        public TupleGeneratorIterator(TupleGenerator generator, int numberOfRecords) {
            this.generator = generator;
            this.generator.reset();
            this.numberOfRecords = numberOfRecords;
            this.counter = 0;
        }

        @Override
        public Tuple2<Integer, String> next(Tuple2<Integer, String> target) {
            if (counter < numberOfRecords) {
                counter++;
                return generator.next(target);
            } else {
                return null;
            }
        }

        @Override
        public Tuple2<Integer, String> next() {
            if (counter < numberOfRecords) {
                counter++;
                return generator.next();
            } else {
                return null;
            }
        }

        public void reset() {
            this.counter = 0;
        }
    }

    public static class TupleConstantValueIterator
            implements MutableObjectIterator<Tuple2<Integer, String>> {

        private int key;
        private String value;

        private final String valueValue;

        private final int numPairs;

        private int pos;

        public TupleConstantValueIterator(int keyValue, String valueValue, int numPairs) {
            this.key = keyValue;
            this.valueValue = valueValue;
            this.numPairs = numPairs;
        }

        @Override
        public Tuple2<Integer, String> next(Tuple2<Integer, String> reuse) {
            if (pos < this.numPairs) {
                this.value = this.valueValue + ' ' + pos;
                reuse.setFields(this.key, this.value);
                pos++;
                return reuse;
            } else {
                return null;
            }
        }

        @Override
        public Tuple2<Integer, String> next() {
            return next(new Tuple2<Integer, String>());
        }

        public void reset() {
            this.pos = 0;
        }
    }

    /**
     * An iterator that returns the Key/Value pairs with identical value a given number of times.
     */
    public static final class ConstantIntIntTuplesIterator
            implements MutableObjectIterator<Tuple2<Integer, Integer>> {

        private final int key;
        private final int value;

        private int numLeft;

        public ConstantIntIntTuplesIterator(int key, int value, int count) {
            this.key = key;
            this.value = value;
            this.numLeft = count;
        }

        @Override
        public Tuple2<Integer, Integer> next(Tuple2<Integer, Integer> reuse) {
            if (this.numLeft > 0) {
                this.numLeft--;
                reuse.setField(this.key, 0);
                reuse.setField(this.value, 1);
                return reuse;
            } else {
                return null;
            }
        }

        @Override
        public Tuple2<Integer, Integer> next() {
            return next(new Tuple2<>(0, 0));
        }
    }

    // ----Tuple2<Integer, String>
    private static final TupleTypeInfo<Tuple2<Integer, String>> typeInfoIntString =
            TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, String.class);

    private static final TypeSerializerFactory<Tuple2<Integer, String>> serializerFactoryIntString =
            new MockTupleSerializerFactory(typeInfoIntString);

    public static TupleTypeInfo<Tuple2<Integer, String>> getIntStringTupleTypeInfo() {
        return typeInfoIntString;
    }

    public static TypeSerializerFactory<Tuple2<Integer, String>>
            getIntStringTupleSerializerFactory() {
        return serializerFactoryIntString;
    }

    public static TypeSerializer<Tuple2<Integer, String>> getIntStringTupleSerializer() {
        return serializerFactoryIntString.getSerializer();
    }

    public static TypeComparator<Tuple2<Integer, String>> getIntStringTupleComparator() {
        return getIntStringTupleTypeInfo()
                .createComparator(new int[] {0}, new boolean[] {true}, 0, null);
    }

    public static MockTuple2Reader<Tuple2<Integer, String>> getIntStringTupleReader() {
        return new MockTuple2Reader<Tuple2<Integer, String>>();
    }

    // ----Tuple2<Integer, Integer>
    private static final TupleTypeInfo<Tuple2<Integer, Integer>> typeInfoIntInt =
            TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class);

    private static final TypeSerializerFactory<Tuple2<Integer, Integer>> serializerFactoryIntInt =
            new MockTupleSerializerFactory(typeInfoIntInt);

    public static TupleTypeInfo<Tuple2<Integer, Integer>> getIntIntTupleTypeInfo() {
        return typeInfoIntInt;
    }

    public static TypeSerializerFactory<Tuple2<Integer, Integer>>
            getIntIntTupleSerializerFactory() {
        return serializerFactoryIntInt;
    }

    public static TypeSerializer<Tuple2<Integer, Integer>> getIntIntTupleSerializer() {
        return getIntIntTupleSerializerFactory().getSerializer();
    }

    public static TypeComparator<Tuple2<Integer, Integer>> getIntIntTupleComparator() {
        return getIntIntTupleTypeInfo()
                .createComparator(new int[] {0}, new boolean[] {true}, 0, null);
    }

    public static MockTuple2Reader<Tuple2<Integer, Integer>> getIntIntTupleReader() {
        return new MockTuple2Reader<>();
    }

    // ----Tuple2<?, ?>
    private static class MockTupleSerializerFactory<T extends Tuple>
            implements TypeSerializerFactory<T> {
        private final TupleTypeInfo<T> info;

        public MockTupleSerializerFactory(TupleTypeInfo<T> info) {
            this.info = info;
        }

        @Override
        public void writeParametersToConfig(Configuration config) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void readParametersFromConfig(Configuration config, ClassLoader cl)
                throws ClassNotFoundException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public TypeSerializer<T> getSerializer() {
            return info.createSerializer(null);
        }

        @Override
        public Class<T> getDataType() {
            return info.getTypeClass();
        }
    }

    public static class MockTuple2Reader<T extends Tuple2> implements MutableObjectIterator<T> {
        private final Tuple2 SENTINEL = new Tuple2();

        private final BlockingQueue<Tuple2> queue;

        public MockTuple2Reader() {
            this.queue = new ArrayBlockingQueue<Tuple2>(32, false);
        }

        public MockTuple2Reader(int size) {
            this.queue = new ArrayBlockingQueue<Tuple2>(size, false);
        }

        @Override
        public T next(T reuse) {
            Tuple2 r = null;
            while (r == null) {
                try {
                    r = queue.take();
                } catch (InterruptedException iex) {
                    throw new RuntimeException("Reader was interrupted.");
                }
            }

            if (r.equals(SENTINEL)) {
                // put the sentinel back, to ensure that repeated calls do not block
                try {
                    queue.put(r);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Reader was interrupted.");
                }
                return null;
            } else {
                reuse.setField(r.getField(0), 0);
                reuse.setField(r.getField(1), 1);
                return reuse;
            }
        }

        @Override
        public T next() {
            Tuple2 r = null;
            while (r == null) {
                try {
                    r = queue.take();
                } catch (InterruptedException iex) {
                    throw new RuntimeException("Reader was interrupted.");
                }
            }

            if (r.equals(SENTINEL)) {
                // put the sentinel back, to ensure that repeated calls do not block
                try {
                    queue.put(r);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Reader was interrupted.");
                }
                return null;
            } else {
                Tuple2 result = new Tuple2(r.f0, r.f1);
                return (T) result;
            }
        }

        public void emit(Tuple2 element) throws InterruptedException {
            queue.put(new Tuple2(element.f0, element.f1));
        }

        public void close() {
            try {
                queue.put(SENTINEL);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class IntPairComparator extends TypeComparator<IntPair> {

        private static final long serialVersionUID = 1L;

        private int reference;

        private final TypeComparator[] comparators = new TypeComparator[] {new IntComparator(true)};

        @Override
        public int hash(IntPair object) {
            return comparators[0].hash(object.getKey());
        }

        @Override
        public void setReference(IntPair toCompare) {
            this.reference = toCompare.getKey();
        }

        @Override
        public boolean equalToReference(IntPair candidate) {
            return candidate.getKey() == this.reference;
        }

        @Override
        public int compareToReference(TypeComparator<IntPair> referencedAccessors) {
            final IntPairComparator comp = (IntPairComparator) referencedAccessors;
            return comp.reference - this.reference;
        }

        @Override
        public int compare(IntPair first, IntPair second) {
            return first.getKey() - second.getKey();
        }

        @Override
        public int compareSerialized(DataInputView source1, DataInputView source2)
                throws IOException {
            return source1.readInt() - source2.readInt();
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
        public void putNormalizedKey(IntPair record, MemorySegment target, int offset, int len) {
            // see IntValue for a documentation of the logic
            final int value = record.getKey() - Integer.MIN_VALUE;

            if (len == 4) {
                target.putIntBigEndian(offset, value);
            } else if (len <= 0) {
            } else if (len < 4) {
                for (int i = 0; len > 0; len--, i++) {
                    target.put(offset + i, (byte) ((value >>> ((3 - i) << 3)) & 0xff));
                }
            } else {
                target.putIntBigEndian(offset, value);
                for (int i = 4; i < len; i++) {
                    target.put(offset + i, (byte) 0);
                }
            }
        }

        @Override
        public boolean invertNormalizedKey() {
            return false;
        }

        @Override
        public IntPairComparator duplicate() {
            return new IntPairComparator();
        }

        @Override
        public int extractKeys(Object record, Object[] target, int index) {
            target[index] = ((IntPair) record).getKey();
            return 1;
        }

        @Override
        public TypeComparator[] getFlatComparators() {
            return comparators;
        }

        @Override
        public boolean supportsSerializationWithKeyNormalization() {
            return true;
        }

        @Override
        public void writeWithKeyNormalization(IntPair record, DataOutputView target)
                throws IOException {
            target.writeInt(record.getKey() - Integer.MIN_VALUE);
            target.writeInt(record.getValue());
        }

        @Override
        public IntPair readWithKeyDenormalization(IntPair reuse, DataInputView source)
                throws IOException {
            reuse.setKey(source.readInt() + Integer.MIN_VALUE);
            reuse.setValue(source.readInt());
            return reuse;
        }
    }
}
