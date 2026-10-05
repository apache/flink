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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.TimeZone;

import static org.apache.flink.table.runtime.functions.VariantCastUtils.toPrintString;
import static org.apache.flink.types.variant.BinaryVariantUtil.primitiveHeader;
import static org.apache.flink.types.variant.BinaryVariantUtil.shortStrHeader;
import static org.assertj.core.api.Assertions.assertThat;

class VariantCastUtilsTest {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private static final VariantBuilder BUILDER = Variant.newBuilder();

    @Test
    void testPrintRendersLikeCast() {
        final Variant variant =
                BUILDER.object()
                        .add(
                                "list",
                                BUILDER.array().add(BUILDER.of("x")).add(BUILDER.ofNull()).build())
                        .build();

        assertThat(toPrintString(variant, UTC))
                .isEqualTo("{list=[x, NULL]}")
                .isEqualTo(
                        VariantCastUtils.toStringValue(variant, UTC, Integer.MAX_VALUE, false)
                                .toString());
    }

    @Test
    void testPrintShowsValuesTheCastRejects() {
        assertThat(toPrintString(BUILDER.of(Double.NaN), UTC)).isEqualTo("NaN");
        assertThat(toPrintString(BUILDER.ofNull(), UTC)).isEqualTo("NULL");
        assertThat(toPrintString(BUILDER.of(new byte[] {(byte) 0xC3, (byte) 0x28}), UTC))
                .isEqualTo("x'c328'");
    }

    @Test
    void testPrintShowsUndecodableNodes() {
        final BinaryVariant ints =
                (BinaryVariant) BUILDER.array().add(BUILDER.of(1)).add(BUILDER.of(2)).build();
        final BinaryVariant strings =
                (BinaryVariant) BUILDER.array().add(BUILDER.of(1)).add(BUILDER.of("xy")).build();

        assertThat(toPrintString(withHeader(ints, primitiveHeader(31)), UTC))
                .isEqualTo("[1, <UNKNOWN>]");
        // Claim a longer string than the buffer holds.
        assertThat(toPrintString(withHeader(strings, shortStrHeader(63)), UTC))
                .isEqualTo("[1, <INVALID>]");
    }

    @Test
    void testPrintShowsTimestampLtzInSessionZone() {
        final Variant variant = BUILDER.of(Instant.parse("2021-09-24T12:34:56.123456Z"));

        assertThat(toPrintString(variant, TimeZone.getTimeZone("Europe/Berlin")))
                .isEqualTo("2021-09-24 14:34:56.123456");
    }

    /** Replaces the header of the array's second element. */
    private static Variant withHeader(final BinaryVariant array, final byte header) {
        final byte[] value = array.getValue().clone();
        value[((BinaryVariant) array.getElement(1)).getPos()] = header;
        return new BinaryVariant(value, array.getMetadata());
    }
}
