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

package org.apache.flink.connector.datagen.table.types;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.util.CollectionUtil;

import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * A {@link GeneratorFunction} that produces random elements of type {@code T}. The index argument
 * is ignored; randomness is sourced from a per-subtask {@link RandomDataGenerator} initialized in
 * {@link #open(SourceReaderContext)}.
 *
 * <p>This is the FLIP-27 replacement for the legacy {@code
 * org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator}. It preserves the legacy
 * null-rate semantics: a value is produced when {@code nullRate < random.nextFloat()}, otherwise
 * {@code null} is emitted.
 */
@Internal
public abstract class RandomGeneratorFunction<T> implements GeneratorFunction<Long, T> {

    private static final long serialVersionUID = 1L;

    protected transient RandomDataGenerator random;
    protected float nullRate;
    protected boolean varLen;

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        this.random = new RandomDataGenerator();
    }

    public RandomGeneratorFunction<T> withNullRate(float nullRate) {
        this.nullRate = nullRate;
        return this;
    }

    public RandomGeneratorFunction<T> withVarLen(boolean varLen) {
        this.varLen = varLen;
        return this;
    }

    protected T nextWithNullRate(Supplier<T> supplier) {
        if (nullRate == 0f || nullRate < ThreadLocalRandom.current().nextFloat()) {
            return supplier.get();
        }
        return null;
    }

    public static RandomGeneratorFunction<Long> longGenerator(long min, long max) {
        return new RandomGeneratorFunction<Long>() {
            @Override
            public Long map(Long value) {
                return nextWithNullRate(() -> random.nextLong(min, max));
            }
        };
    }

    public static RandomGeneratorFunction<Integer> intGenerator(int min, int max) {
        return new RandomGeneratorFunction<Integer>() {
            @Override
            public Integer map(Long value) {
                return nextWithNullRate(() -> random.nextInt(min, max));
            }
        };
    }

    public static RandomGeneratorFunction<Short> shortGenerator(short min, short max) {
        return new RandomGeneratorFunction<Short>() {
            @Override
            public Short map(Long value) {
                return nextWithNullRate(() -> (short) random.nextInt(min, max));
            }
        };
    }

    public static RandomGeneratorFunction<Byte> byteGenerator(byte min, byte max) {
        return new RandomGeneratorFunction<Byte>() {
            @Override
            public Byte map(Long value) {
                return nextWithNullRate(() -> (byte) random.nextInt(min, max));
            }
        };
    }

    public static RandomGeneratorFunction<Float> floatGenerator(float min, float max) {
        return new RandomGeneratorFunction<Float>() {
            @Override
            public Float map(Long value) {
                return nextWithNullRate(() -> (float) random.nextUniform(min, max));
            }
        };
    }

    public static RandomGeneratorFunction<Double> doubleGenerator(double min, double max) {
        return new RandomGeneratorFunction<Double>() {
            @Override
            public Double map(Long value) {
                return nextWithNullRate(() -> random.nextUniform(min, max));
            }
        };
    }

    public static RandomGeneratorFunction<String> stringGenerator(int len) {
        return new RandomGeneratorFunction<String>() {
            @Override
            public String map(Long value) {
                return nextWithNullRate(() -> random.nextHexString(len));
            }
        };
    }

    public static RandomGeneratorFunction<Boolean> booleanGenerator() {
        return new RandomGeneratorFunction<Boolean>() {
            @Override
            public Boolean map(Long value) {
                return nextWithNullRate(() -> random.nextInt(0, 1) == 0);
            }
        };
    }

    public static <T> RandomGeneratorFunction<T[]> arrayGenerator(
            GeneratorFunction<Long, T> generator, int len) {
        return new RandomGeneratorFunction<T[]>() {

            @Override
            public void open(SourceReaderContext readerContext) throws Exception {
                super.open(readerContext);
                generator.open(readerContext);
            }

            @Override
            public T[] map(Long value) throws Exception {
                @SuppressWarnings("unchecked")
                T[] array = (T[]) new Object[len];

                for (int i = 0; i < len; i++) {
                    array[i] = generator.map(value);
                }

                return array;
            }
        };
    }

    public static <K, V> RandomGeneratorFunction<Map<K, V>> mapGenerator(
            GeneratorFunction<Long, K> key, GeneratorFunction<Long, V> val, int size) {
        return new RandomGeneratorFunction<Map<K, V>>() {

            @Override
            public void open(SourceReaderContext readerContext) throws Exception {
                super.open(readerContext);
                key.open(readerContext);
                val.open(readerContext);
            }

            @Override
            public Map<K, V> map(Long value) throws Exception {
                Map<K, V> map = CollectionUtil.newHashMapWithExpectedSize(size);
                for (int i = 0; i < size; i++) {
                    map.put(key.map(value), val.map(value));
                }
                return map;
            }
        };
    }
}
