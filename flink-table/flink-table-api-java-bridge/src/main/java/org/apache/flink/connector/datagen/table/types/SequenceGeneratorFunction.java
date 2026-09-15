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
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.primitives.Longs;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * A {@link GeneratorFunction} that emits each value from a closed {@code [start, end]} interval
 * exactly once when driven with indices in {@code [0, end - start]}.
 *
 * <p>This is the FLIP-27 replacement for the legacy {@code
 * org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator}. Compared to the
 * legacy implementation, the per-subtask checkpointed deque of remaining values is no longer
 * needed: the {@code NumberSequenceSource} that drives the FLIP-27 {@code DataGeneratorSource}
 * persists the remaining index range itself, and this function is a pure mapping {@code idx ->
 * convert(start + idx)}.
 *
 * <p>The factory wiring guarantees that the source-level {@code count} never exceeds {@link
 * #getTotalCount()}, so the {@code start + idx} computation never wraps past {@code end}.
 *
 * @param <T> The produced element type.
 */
@Internal
public abstract class SequenceGeneratorFunction<T> implements GeneratorFunction<Long, T> {

    private static final long serialVersionUID = 1L;

    private final long start;
    private final long end;

    protected SequenceGeneratorFunction(long start, long end) {
        Preconditions.checkArgument(end >= start, "end (%s) must be >= start (%s)", end, start);
        this.start = start;
        this.end = end;
    }

    /**
     * Returns the total number of distinct values produced by this generator, i.e. {@code end -
     * start + 1}.
     */
    public long getTotalCount() {
        return end - start + 1;
    }

    @Override
    public final T map(Long value) {
        return convert(start + value);
    }

    /** Converts the produced {@code long} value to the desired output type. */
    protected abstract T convert(long value);

    public static SequenceGeneratorFunction<Long> longGenerator(long start, long end) {
        return new SequenceGeneratorFunction<Long>(start, end) {
            @Override
            protected Long convert(long value) {
                return value;
            }
        };
    }

    public static SequenceGeneratorFunction<Integer> intGenerator(int start, int end) {
        return new SequenceGeneratorFunction<Integer>(start, end) {
            @Override
            protected Integer convert(long value) {
                return (int) value;
            }
        };
    }

    public static SequenceGeneratorFunction<Short> shortGenerator(short start, short end) {
        return new SequenceGeneratorFunction<Short>(start, end) {
            @Override
            protected Short convert(long value) {
                return (short) value;
            }
        };
    }

    public static SequenceGeneratorFunction<Byte> byteGenerator(byte start, byte end) {
        return new SequenceGeneratorFunction<Byte>(start, end) {
            @Override
            protected Byte convert(long value) {
                return (byte) value;
            }
        };
    }

    public static SequenceGeneratorFunction<Float> floatGenerator(short start, short end) {
        return new SequenceGeneratorFunction<Float>(start, end) {
            @Override
            protected Float convert(long value) {
                return (float) value;
            }
        };
    }

    public static SequenceGeneratorFunction<Double> doubleGenerator(int start, int end) {
        return new SequenceGeneratorFunction<Double>(start, end) {
            @Override
            protected Double convert(long value) {
                return (double) value;
            }
        };
    }

    public static SequenceGeneratorFunction<BigDecimal> bigDecimalGenerator(
            int start, int end, int precision, int scale) {
        return new SequenceGeneratorFunction<BigDecimal>(start, end) {
            @Override
            protected BigDecimal convert(long value) {
                BigDecimal decimal = new BigDecimal((double) value, new MathContext(precision));
                return decimal.setScale(scale, RoundingMode.DOWN);
            }
        };
    }

    public static SequenceGeneratorFunction<String> stringGenerator(long start, long end) {
        return new SequenceGeneratorFunction<String>(start, end) {
            @Override
            protected String convert(long value) {
                return Long.toString(value);
            }
        };
    }

    public static SequenceGeneratorFunction<StringData> stringDataGenerator(long start, long end) {
        return new SequenceGeneratorFunction<StringData>(start, end) {
            @Override
            protected StringData convert(long value) {
                return StringData.fromString(Long.toString(value));
            }
        };
    }

    public static SequenceGeneratorFunction<byte[]> bytesGenerator(long start, long end) {
        return new SequenceGeneratorFunction<byte[]>(start, end) {
            @Override
            protected byte[] convert(long value) {
                return Longs.toByteArray(value);
            }
        };
    }
}
