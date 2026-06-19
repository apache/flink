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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinaryVariantInternalBuilderTest {

    @Test
    void testParseScalarJson() throws IOException {
        assertThat(BinaryVariantInternalBuilder.parseJson("1", false).getByte())
                .isEqualTo((byte) 1);
        short s = (short) (Byte.MAX_VALUE + 1L);
        assertThat(BinaryVariantInternalBuilder.parseJson(String.valueOf(s), false).getShort())
                .isEqualTo(s);
        int i = (int) (Short.MAX_VALUE + 1L);
        assertThat(BinaryVariantInternalBuilder.parseJson(String.valueOf(i), false).getInt())
                .isEqualTo(i);
        long l = Integer.MAX_VALUE + 1L;
        assertThat(BinaryVariantInternalBuilder.parseJson(String.valueOf(l), false).getLong())
                .isEqualTo(l);

        BigDecimal bigDecimal = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
        assertThat(
                        BinaryVariantInternalBuilder.parseJson(bigDecimal.toPlainString(), false)
                                .getDecimal())
                .isEqualTo(bigDecimal);

        assertThat(BinaryVariantInternalBuilder.parseJson("1.123", false).getDecimal())
                .isEqualTo(BigDecimal.valueOf(1.123));
        assertThat(
                        BinaryVariantInternalBuilder.parseJson(
                                        String.valueOf(Double.MAX_VALUE), false)
                                .getDouble())
                .isEqualTo(Double.MAX_VALUE);

        assertThat(BinaryVariantInternalBuilder.parseJson("\"hello\"", false).getString())
                .isEqualTo("hello");

        assertThat(BinaryVariantInternalBuilder.parseJson("true", false).getBoolean()).isTrue();

        assertThat(BinaryVariantInternalBuilder.parseJson("false", false).getBoolean()).isFalse();

        assertThat(BinaryVariantInternalBuilder.parseJson("null", false).isNull()).isTrue();
    }

    @Test
    void testParseJsonArray() throws IOException {
        BinaryVariant variant = BinaryVariantInternalBuilder.parseJson("[]", false);
        assertThat(variant.getElement(0)).isNull();

        variant = BinaryVariantInternalBuilder.parseJson("[1,\"hello\",3.1, null]", false);
        assertThat(variant.getElement(0).getByte()).isEqualTo((byte) 1);
        assertThat(variant.getElement(1).getString()).isEqualTo("hello");
        assertThat(variant.getElement(2).getDecimal()).isEqualTo(BigDecimal.valueOf(3.1));
        assertThat(variant.getElement(3).isNull()).isTrue();

        variant = BinaryVariantInternalBuilder.parseJson("[1,[\"hello\",[3.1]]]", false);
        assertThat(variant.getElement(0).getByte()).isEqualTo((byte) 1);
        assertThat(variant.getElement(1).getElement(0).getString()).isEqualTo("hello");
        assertThat(variant.getElement(1).getElement(1).getElement(0).getDecimal())
                .isEqualTo(BigDecimal.valueOf(3.1));
    }

    @Test
    void testParseJsonObject() throws IOException {
        BinaryVariant variant = BinaryVariantInternalBuilder.parseJson("{}", false);
        assertThat(variant.getField("a")).isNull();

        variant =
                BinaryVariantInternalBuilder.parseJson(
                        "{\"a\":1,\"b\":\"hello\",\"c\":3.1}", false);

        assertThat(variant.getField("a").getByte()).isEqualTo((byte) 1);
        assertThat(variant.getField("b").getString()).isEqualTo("hello");
        assertThat(variant.getField("c").getDecimal()).isEqualTo(BigDecimal.valueOf(3.1));

        variant =
                BinaryVariantInternalBuilder.parseJson(
                        "{\"a\":1,\"b\":{\"c\":\"hello\",\"d\":[3.1]}}", false);
        assertThat(variant.getField("a").getByte()).isEqualTo((byte) 1);
        assertThat(variant.getField("b").getField("c").getString()).isEqualTo("hello");
        assertThat(variant.getField("b").getField("d").getElement(0).getDecimal())
                .isEqualTo(BigDecimal.valueOf(3.1));

        assertThatThrownBy(
                        () ->
                                BinaryVariantInternalBuilder.parseJson(
                                        "{\"k1\":1,\"k1\":2,\"k2\":1.5}", false))
                .isInstanceOf(VariantTypeException.class)
                .hasMessage("VARIANT_DUPLICATE_KEY");

        variant = BinaryVariantInternalBuilder.parseJson("{\"k1\":1,\"k1\":2,\"k2\":1.5}", true);
        assertThat(variant.getField("k1").getByte()).isEqualTo((byte) 2);
        assertThat(variant.getField("k2").getDecimal()).isEqualTo(BigDecimal.valueOf(1.5));
    }

    @Test
    void testAppendFloat() {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(false);
        ArrayList<Float> floatList = new ArrayList<>(Collections.nCopies(25, 4.2f));

        assertThatCode(() -> floatList.forEach(builder::appendFloat)).doesNotThrowAnyException();
    }
}
