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

package org.apache.flink.types.variant;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

/** Builder for binary encoded variant. */
@Internal
public class BinaryVariantBuilder implements VariantBuilder {

    @Override
    public Variant of(byte b) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendByte(b);
        return builder.build();
    }

    @Override
    public Variant of(short s) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendShort(s);
        return builder.build();
    }

    @Override
    public Variant of(int i) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendInt(i);
        return builder.build();
    }

    @Override
    public Variant of(long l) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendLong(l);
        return builder.build();
    }

    @Override
    public Variant of(String s) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendString(s);
        return builder.build();
    }

    @Override
    public Variant of(double d) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendDouble(d);
        return builder.build();
    }

    @Override
    public Variant of(float f) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendFloat(f);
        return builder.build();
    }

    @Override
    public Variant of(byte[] bytes) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendBinary(bytes);
        return builder.build();
    }

    @Override
    public Variant of(boolean b) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendBoolean(b);
        return builder.build();
    }

    @Override
    public Variant of(BigDecimal bigDecimal) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendDecimal(bigDecimal);
        return builder.build();
    }

    @Override
    public Variant of(Instant instant) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        if (instant.getNano() % 1000 == 0) {
            builder.appendTimestampLtz(microsSinceEpoch(instant));
        } else {
            builder.appendTimestampLtzNanos(nanosSinceEpoch(instant));
        }
        return builder.build();
    }

    @Override
    public Variant of(LocalDate localDate) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendDate((int) localDate.toEpochDay());
        return builder.build();
    }

    @Override
    public Variant of(LocalDateTime localDateTime) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
        if (localDateTime.getNano() % 1000 == 0) {
            builder.appendTimestamp(microsSinceEpoch(instant));
        } else {
            builder.appendTimestampNanos(nanosSinceEpoch(instant));
        }
        return builder.build();
    }

    private static long microsSinceEpoch(Instant instant) {
        try {
            return Math.addExact(
                    Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                    instant.getNano() / 1000);
        } catch (ArithmeticException e) {
            throw new VariantTypeException(
                    String.format(
                            "%s is outside the range supported by microsecond precision variant "
                                    + "timestamps.",
                            instant));
        }
    }

    private static long nanosSinceEpoch(Instant instant) {
        try {
            return ChronoUnit.NANOS.between(Instant.EPOCH, instant);
        } catch (ArithmeticException e) {
            throw new VariantTypeException(
                    String.format(
                            "%s is outside the +/-292 year range (1677-09-21 to 2262-04-11) "
                                    + "supported by nanosecond precision variant timestamps. Use "
                                    + "microsecond precision instead.",
                            instant));
        }
    }

    @Override
    public Variant of(LocalTime localTime) {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendTime(localTime.toNanoOfDay() / 1000);
        return builder.build();
    }

    @Override
    public Variant ofNull() {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        builder.appendNull();
        return builder.build();
    }

    @Override
    public VariantObjectBuilder object() {
        return object(false);
    }

    @Override
    public VariantObjectBuilder object(boolean allowDuplicateKeys) {
        return new VariantObjectBuilder(allowDuplicateKeys);
    }

    /** Get the builder for a variant array. */
    public VariantArrayBuilder array() {
        return new VariantArrayBuilder();
    }

    /** Builder for a variant object. */
    @PublicEvolving
    public static class VariantObjectBuilder implements VariantBuilder.VariantObjectBuilder {
        private final BinaryVariantInternalBuilder builder;
        private final ArrayList<BinaryVariantInternalBuilder.FieldEntry> entries =
                new ArrayList<>();

        public VariantObjectBuilder(boolean allowDuplicateKeys) {
            builder = new BinaryVariantInternalBuilder(allowDuplicateKeys);
        }

        @Override
        public VariantObjectBuilder add(String key, Variant value) {
            int id = builder.addKey(key);
            entries.add(
                    new BinaryVariantInternalBuilder.FieldEntry(key, id, builder.getWritePos()));
            builder.appendVariant((BinaryVariant) value);
            return this;
        }

        @Override
        public Variant build() {
            builder.finishWritingObject(0, entries);
            return builder.build();
        }
    }

    /** Builder for a variant array. */
    @PublicEvolving
    public static class VariantArrayBuilder implements VariantBuilder.VariantArrayBuilder {

        private final BinaryVariantInternalBuilder builder;
        private final ArrayList<Integer> offsets = new ArrayList<>();

        public VariantArrayBuilder() {
            builder = new BinaryVariantInternalBuilder(false);
        }

        @Override
        public VariantArrayBuilder add(Variant value) {
            offsets.add(builder.getWritePos());
            builder.appendVariant((BinaryVariant) value);
            return this;
        }

        @Override
        public Variant build() {
            builder.finishWritingArray(0, offsets);
            return builder.build();
        }
    }
}
