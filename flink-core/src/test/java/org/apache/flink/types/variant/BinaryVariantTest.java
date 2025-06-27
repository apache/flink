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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinaryVariantTest {

    private BinaryVariantBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new BinaryVariantBuilder();
    }

    @Test
    void testScalarVariant() {

        assertThat(builder.of((byte) 10).isPrimitive()).isTrue();
        assertThat(builder.of((byte) 10).isNull()).isFalse();
        assertThat(builder.of((byte) 10).isArray()).isFalse();
        assertThat(builder.of((byte) 10).isObject()).isFalse();
        assertThat(builder.of((byte) 10).getType()).isEqualTo(Variant.Type.TINYINT);

        assertThat(builder.of((byte) 10).getByte()).isEqualTo((byte) 10);
        assertThat(builder.of((byte) 10).get()).isEqualTo((byte) 10);
        assertThat((byte) builder.of((byte) 10).getAs()).isEqualTo((byte) 10);

        assertThat(builder.of((short) 10).getShort()).isEqualTo((short) 10);
        assertThat(builder.of((short) 10).get()).isEqualTo((short) 10);

        assertThat(builder.of(10).getInt()).isEqualTo(10);
        assertThat(builder.of(10).get()).isEqualTo(10);

        assertThat(builder.of(10L).getLong()).isEqualTo(10L);
        assertThat(builder.of(10L).get()).isEqualTo(10L);

        assertThat(builder.of(10.0).getDouble()).isEqualTo(10.0d);
        assertThat(builder.of(10.0).get()).isEqualTo(10.0d);

        assertThat(builder.of(10.0f).getFloat()).isEqualTo(10.0f);
        assertThat(builder.of(10.0f).get()).isEqualTo(10.0f);

        assertThat(builder.of("hello").getString()).isEqualTo("hello");
        assertThat(builder.of("hello").get()).isEqualTo("hello");

        assertThat(builder.of("hello".getBytes()).getBytes()).isEqualTo("hello".getBytes());
        assertThat(builder.of("hello".getBytes()).get()).isEqualTo("hello".getBytes());

        assertThat(builder.of(true).getBoolean()).isTrue();
        assertThat(builder.of(true).get()).isEqualTo(true);

        assertThat(builder.of(BigDecimal.valueOf(100)).getDecimal())
                .isEqualByComparingTo(BigDecimal.valueOf(100));
        assertThat((BigDecimal) builder.of(BigDecimal.valueOf(100)).get())
                .isEqualByComparingTo(BigDecimal.valueOf(100));

        Instant instant = Instant.now().truncatedTo(ChronoUnit.MICROS);
        assertThat(builder.of(instant).getInstant()).isEqualTo(instant);
        assertThat(builder.of(instant).get()).isEqualTo(instant);

        LocalDateTime localDateTime = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS);
        assertThat(builder.of(localDateTime).getDateTime()).isEqualTo(localDateTime);
        assertThat(builder.of(localDateTime).get()).isEqualTo(localDateTime);

        LocalDate localDate = LocalDate.now();
        assertThat(builder.of(localDate).getDate()).isEqualTo(localDate);
        assertThat(builder.of(localDate).get()).isEqualTo(localDate);

        assertThat(builder.ofNull().get()).isEqualTo(null);
        assertThat(builder.ofNull().isNull()).isTrue();
    }

    @Test
    void testArrayVariant() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Variant variant =
                builder.array()
                        .add(builder.of(1))
                        .add(builder.of("hello"))
                        .add(builder.of(now))
                        .add(builder.array().add(builder.of("hello2")).add(builder.of(10f)).build())
                        .add(builder.ofNull())
                        .build();

        assertThat(variant.isArray()).isTrue();
        assertThat(variant.isPrimitive()).isFalse();
        assertThat(variant.isObject()).isFalse();
        assertThat(variant.getType()).isEqualTo(Variant.Type.ARRAY);

        assertThat(variant.getElement(-1)).isNull();
        assertThat(variant.getElement(0).getInt()).isEqualTo(1);
        assertThat(variant.getElement(1).getString()).isEqualTo("hello");
        assertThat(variant.getElement(2).getInstant()).isEqualTo(now);
        assertThat(variant.getElement(3).getElement(0).getString()).isEqualTo("hello2");
        assertThat(variant.getElement(3).getElement(1).getFloat()).isEqualTo(10f);
        assertThat(variant.getElement(4).isNull()).isTrue();
        assertThat(variant.getElement(5)).isNull();
    }

    @Test
    void testObjectVariant() {
        Variant variant =
                builder.object()
                        .add(
                                "list",
                                builder.array().add(builder.of("hello")).add(builder.of(1)).build())
                        .add(
                                "object",
                                builder.object()
                                        .add("ss", builder.of((short) 1))
                                        .add("ff", builder.of(10.0f))
                                        .build())
                        .add("bb", builder.of((byte) 10))
                        .build();

        assertThat(variant.isArray()).isFalse();
        assertThat(variant.isPrimitive()).isFalse();
        assertThat(variant.isObject()).isTrue();
        assertThat(variant.getType()).isEqualTo(Variant.Type.OBJECT);

        assertThat(variant.getField("list").isArray()).isTrue();
        assertThat(variant.getField("list").getElement(0).getString()).isEqualTo("hello");
        assertThat(variant.getField("list").getElement(1).getInt()).isEqualTo(1);

        assertThat(variant.getField("object").isObject()).isTrue();
        assertThat(variant.getField("object").getField("ss").getShort()).isEqualTo((short) 1);
        assertThat(variant.getField("object").getField("ff").getFloat()).isEqualTo((10.0f));

        assertThat(variant.getField("bb").getByte()).isEqualTo((byte) 10);
        assertThat(variant.getField("non_exist")).isNull();

        BinaryVariantBuilder.VariantObjectBuilder objectBuilder = builder.object();

        for (int i = 0; i < 100; i++) {
            objectBuilder.add(String.valueOf(i), builder.of(i));
        }
        variant = objectBuilder.build();
        for (int i = 0; i < 100; i++) {
            assertThat(variant.getField(String.valueOf(i)).getInt()).isEqualTo(i);
        }
    }

    @Test
    void testDuplicatedKeyObjectVariant() {
        assertThatThrownBy(
                        () ->
                                builder.object(false)
                                        .add("k", builder.of((byte) 10))
                                        .add("k", builder.of("hello"))
                                        .build())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("VARIANT_DUPLICATE_KEY");

        Variant variant =
                builder.object(true)
                        .add("k", builder.of((byte) 10))
                        .add("k", builder.of("hello"))
                        .add("k1", builder.of(10))
                        .build();

        assertThat(variant.getField("k").getString()).isEqualTo("hello");
        assertThat(variant.getField("k1").getInt()).isEqualTo(10);
    }

    @Test
    void testToJsonScalar() {
        Instant instant = Instant.EPOCH;
        LocalDateTime localDateTime = LocalDateTime.of(2000, 1, 1, 0, 0);
        LocalDate localDate = LocalDate.of(2000, 1, 1);

        assertThat(builder.of((byte) 1).toJson()).isEqualTo("1");
        assertThat(builder.of((short) 1).toJson()).isEqualTo("1");
        assertThat(builder.of(1L).toJson()).isEqualTo("1");
        assertThat(builder.of(1).toJson()).isEqualTo("1");
        assertThat(builder.of("hello").toJson()).isEqualTo("\"hello\"");
        assertThat(builder.of(true).toJson()).isEqualTo("true");
        assertThat(builder.of(10.0f).toJson()).isEqualTo("10.0");
        assertThat(builder.of(10.0d).toJson()).isEqualTo("10.0");
        assertThat(builder.of(BigDecimal.valueOf(100)).toJson()).isEqualTo("100");
        assertThat(builder.of(instant).toJson()).isEqualTo("\"1970-01-01T00:00:00+00:00\"");
        assertThat(builder.of(localDateTime).toJson()).isEqualTo("\"2000-01-01T00:00:00\"");
        assertThat(builder.of(localDate).toJson()).isEqualTo("\"2000-01-01\"");
        assertThat(builder.of("hello".getBytes()).toJson()).isEqualTo("\"aGVsbG8=\"");
        assertThat(builder.ofNull().toJson()).isEqualTo("null");
    }

    @Test
    void testToJsonNested() {
        Variant variant =
                builder.object()
                        .add(
                                "list",
                                builder.array().add(builder.of("hello")).add(builder.of(1)).build())
                        .add(
                                "object",
                                builder.object()
                                        .add("ss", builder.of((short) 1))
                                        .add("ff", builder.of(10.0f))
                                        .build())
                        .build();

        String json = variant.toJson();
        assertThat(json)
                .isEqualTo("{" + "\"list\":[\"hello\",1]," + "\"object\":{\"ff\":10.0,\"ss\":1}}");
    }

    @Test
    void testVariantException() {
        assertThatThrownBy(() -> new BinaryVariant(new byte[0], new byte[0]))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("MALFORMED_VARIANT");

        byte[] meta = new byte[1];
        meta[0] = (byte) 0x02;
        assertThatThrownBy(() -> new BinaryVariant(new byte[1], meta))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("MALFORMED_VARIANT");

        byte[] oversize = new byte[0xFFFFFF + 2];
        meta[0] = (byte) 0x01;
        oversize[0] = (byte) 0x01;
        assertThatThrownBy(() -> new BinaryVariant(oversize, meta))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("VARIANT_CONSTRUCTOR_SIZE_LIMIT");

        assertThatThrownBy(() -> new BinaryVariant(new byte[1], oversize))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("VARIANT_CONSTRUCTOR_SIZE_LIMIT");
    }

    @Test
    void testGetThrowException() {
        Variant variant = builder.of(10f);
        assertThatThrownBy(variant::getDouble)
                .isInstanceOf(VariantTypeException.class)
                .hasMessage("Expected type DOUBLE but got FLOAT");
    }
}
