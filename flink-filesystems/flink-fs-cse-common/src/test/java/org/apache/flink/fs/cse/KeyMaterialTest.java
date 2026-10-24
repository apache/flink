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

package org.apache.flink.fs.cse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KeyMaterial}. */
class KeyMaterialTest {

    private static final byte[] VALID_KEY = {1, 2, 3};

    @Test
    void shouldStoreKeyIdAndVersion() {
        final KeyMaterial km = new KeyMaterial("id-1", "v2", VALID_KEY);
        assertThat(km.getKeyId()).isEqualTo("id-1");
        assertThat(km.getVersion()).isEqualTo("v2");
    }

    @Test
    void shouldDefensivelyCopyMaterialOnConstruction() {
        final byte[] original = {10, 20, 30};
        final KeyMaterial km = new KeyMaterial("id", "v1", original);
        original[0] = 99;
        assertThat(km.getMaterial()).containsExactly(10, 20, 30);
    }

    @Test
    void shouldDefensivelyCopyMaterialOnGet() {
        final KeyMaterial km = new KeyMaterial("id", "v1", new byte[] {10, 20, 30});
        final byte[] first = km.getMaterial();
        first[0] = 99;
        assertThat(km.getMaterial()).containsExactly(10, 20, 30);
    }

    @ParameterizedTest
    @MethodSource("nullArgs")
    void shouldThrowOnNullArg(
            final String keyId,
            final String version,
            final byte[] material,
            final String expectedMessage) {
        assertThatThrownBy(() -> new KeyMaterial(keyId, version, material))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining(expectedMessage);
    }

    private static Stream<Arguments> nullArgs() {
        return Stream.of(
                Arguments.of(null, "v1", VALID_KEY, "keyId"),
                Arguments.of("id", null, VALID_KEY, "version"),
                Arguments.of("id", "v1", null, "material"));
    }

    @Test
    void shouldThrowOnEmptyMaterial() {
        assertThatThrownBy(() -> new KeyMaterial("id", "v1", new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void shouldBeEqualForSameFields() {
        final KeyMaterial a = new KeyMaterial("id-1", "v1", VALID_KEY);
        final KeyMaterial b = new KeyMaterial("id-1", "v1", VALID_KEY);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void shouldNotBeEqualWhenFieldDiffers() {
        final KeyMaterial base = new KeyMaterial("id-1", "v1", VALID_KEY);
        assertThat(base).isNotEqualTo(new KeyMaterial("id-2", "v1", VALID_KEY));
        assertThat(base).isNotEqualTo(new KeyMaterial("id-1", "v2", VALID_KEY));
    }

    @Test
    void equalsShouldNotCompareMaterial() {
        final KeyMaterial a = new KeyMaterial("id-1", "v1", new byte[] {1, 2, 3});
        final KeyMaterial b = new KeyMaterial("id-1", "v1", new byte[] {9, 8, 7});
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void shouldRedactMaterialInToString() {
        final KeyMaterial km = new KeyMaterial("id-1", "v2", new byte[] {1, 2, 3});
        final String str = km.toString();
        assertThat(str).contains("id-1").contains("v2").doesNotContain("1, 2, 3");
    }
}
