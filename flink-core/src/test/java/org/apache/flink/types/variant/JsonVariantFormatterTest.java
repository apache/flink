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

import static org.apache.flink.types.variant.BinaryVariantUtil.primitiveHeader;
import static org.apache.flink.types.variant.BinaryVariantUtil.shortStrHeader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonVariantFormatterTest {

    private static final BinaryVariantBuilder BUILDER = new BinaryVariantBuilder();

    @Test
    void testValidValueIsTheSameInToJsonAndToString() {
        final Variant variant =
                BUILDER.object()
                        .add("a", BUILDER.of("x"))
                        .add(
                                "list",
                                BUILDER.array().add(BUILDER.of(1)).add(BUILDER.ofNull()).build())
                        .build();

        assertThat(variant.toJson()).isEqualTo("{\"a\":\"x\",\"list\":[1,null]}");
        assertThat(variant.toString()).isEqualTo(variant.toJson());
        assertThat(variant.getField("list").toJson()).isEqualTo("[1,null]");
    }

    @Test
    void testNonFiniteNumbers() {
        final Variant variant =
                BUILDER.array()
                        .add(BUILDER.of(Double.NaN))
                        .add(BUILDER.of(Double.NEGATIVE_INFINITY))
                        .add(BUILDER.of(Float.POSITIVE_INFINITY))
                        .build();

        assertThat(variant.toString()).isEqualTo("[\"NaN\",\"-Infinity\",\"Infinity\"]");
        assertThatThrownBy(variant::toJson)
                .hasMessage("Non-finite value 'NaN' cannot be serialized to JSON.");
    }

    @Test
    void testUnknownType() {
        final BinaryVariant array =
                (BinaryVariant) BUILDER.array().add(BUILDER.of(1)).add(BUILDER.of(2)).build();
        final Variant variant = withHeader(array, primitiveHeader(31));

        assertThat(variant.toString()).isEqualTo("[1,\"<UNKNOWN>\"]");
        assertThatThrownBy(variant::toJson).hasMessage("UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT, id: 31");
    }

    @Test
    void testMalformedValue() {
        final BinaryVariant array =
                (BinaryVariant) BUILDER.array().add(BUILDER.of(1)).add(BUILDER.of("xy")).build();
        // Claim a longer string than the buffer holds.
        final Variant variant = withHeader(array, shortStrHeader(63));

        assertThat(variant.toString()).isEqualTo("[1,\"<INVALID>\"]");
        assertThatThrownBy(variant::toJson).hasMessage("MALFORMED_VARIANT");
    }

    @Test
    void testFailedContainerDropsItsPartialOutput() {
        final BinaryVariant array =
                (BinaryVariant)
                        BUILDER.array()
                                .add(BUILDER.of(1))
                                .add(BUILDER.object().add("a", BUILDER.of(2)).build())
                                .build();
        // Without its key dictionary the object fails after it has written "{".
        final byte[] noKeys = ((BinaryVariant) BUILDER.of(1)).getMetadata();
        final Variant variant = new BinaryVariant(array.getValue(), noKeys);

        assertThat(variant.toString()).isEqualTo("[1,\"<INVALID>\"]");
    }

    /** Replaces the header of the array's second element. */
    private static Variant withHeader(final BinaryVariant array, final byte header) {
        final byte[] value = array.getValue().clone();
        value[((BinaryVariant) array.getElement(1)).getPos()] = header;
        return new BinaryVariant(value, array.getMetadata());
    }
}
