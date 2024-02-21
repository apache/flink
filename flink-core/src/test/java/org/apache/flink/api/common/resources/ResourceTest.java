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

package org.apache.flink.api.common.resources;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Resource}. */
@SuppressWarnings("rawtypes")
class ResourceTest {

    @Test
    void testConstructorValid() {
        final Resource v1 = new TestResource(0.1);
        assertTestResourceValueEquals(0.1, v1);

        final Resource v2 = new TestResource(BigDecimal.valueOf(0.1));
        assertTestResourceValueEquals(0.1, v2);
    }

    @Test
    void testConstructorInvalidValue() {
        assertThatThrownBy(() -> new TestResource(-0.1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEquals() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new TestResource(0.1);
        final Resource v3 = new TestResource(0.2);
        assertThat(v2).isEqualTo(v1);
        assertThat(v3).isNotEqualTo(v1);
    }

    @Test
    void testEqualsIgnoringScale() {
        final Resource v1 = new TestResource(new BigDecimal("0.1"));
        final Resource v2 = new TestResource(new BigDecimal("0.10"));
        assertThat(v2).isEqualTo(v1);
    }

    @Test
    void testHashCodeIgnoringScale() {
        final Resource v1 = new TestResource(new BigDecimal("0.1"));
        final Resource v2 = new TestResource(new BigDecimal("0.10"));
        assertThat(v2).hasSameHashCodeAs(v1);
    }

    @Test
    void testMerge() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new TestResource(0.2);
        assertTestResourceValueEquals(0.3, v1.merge(v2));
    }

    @Test
    void testMergeErrorOnDifferentTypes() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new CPUResource(0.1);
        // v1.merge(v2);
        assertThatThrownBy(() -> v1.merge(v2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSubtract() {
        final Resource v1 = new TestResource(0.2);
        final Resource v2 = new TestResource(0.1);
        assertTestResourceValueEquals(0.1, v1.subtract(v2));
    }

    @Test
    void testSubtractLargerValue() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new TestResource(0.2);
        assertThatThrownBy(() -> v1.subtract(v2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSubtractErrorOnDifferentTypes() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new CPUResource(0.1);
        assertThatThrownBy(() -> v1.subtract(v2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDivide() {
        final Resource resource = new TestResource(0.04);
        final BigDecimal by = BigDecimal.valueOf(0.1);
        assertTestResourceValueEquals(0.4, resource.divide(by));
    }

    @Test
    void testDivideNegative() {
        final Resource resource = new TestResource(1.2);
        final BigDecimal by = BigDecimal.valueOf(-0.5);
        assertThatThrownBy(() -> resource.divide(by)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDivideInteger() {
        final Resource resource = new TestResource(0.12);
        final int by = 4;
        assertTestResourceValueEquals(0.03, resource.divide(by));
    }

    @Test
    void testDivideNegativeInteger() {
        final Resource resource = new TestResource(1.2);
        final int by = -5;
        assertThatThrownBy(() -> resource.divide(by)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMultiply() {
        final Resource resource = new TestResource(0.3);
        final BigDecimal by = BigDecimal.valueOf(0.2);
        assertTestResourceValueEquals(0.06, resource.multiply(by));
    }

    @Test
    void testMutiplyNegative() {
        final Resource resource = new TestResource(0.3);
        final BigDecimal by = BigDecimal.valueOf(-0.2);
        assertThatThrownBy(() -> resource.multiply(by))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testMultiplyInteger() {
        final Resource resource = new TestResource(0.3);
        final int by = 2;
        assertTestResourceValueEquals(0.6, resource.multiply(by));
    }

    @Test
    void testMutiplyNegativeInteger() {
        final Resource resource = new TestResource(0.3);
        final int by = -2;
        assertThatThrownBy(() -> resource.multiply(by))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testIsZero() {
        final Resource resource1 = new TestResource(0.0);
        final Resource resource2 = new TestResource(1.0);

        assertThat(resource1.isZero()).isTrue();
        assertThat(resource2.isZero()).isFalse();
    }

    @Test
    void testCompareTo() {
        final Resource resource1 = new TestResource(0.0);
        final Resource resource2 = new TestResource(0.0);
        final Resource resource3 = new TestResource(1.0);

        assertThat(resource1.compareTo(resource1)).isZero();
        assertThat(resource1.compareTo(resource2)).isZero();
        assertThat(resource1.compareTo(resource3)).isNegative();
        assertThat(resource3.compareTo(resource1)).isPositive();
    }

    @Test
    void testCompareToFailNull() {
        // new TestResource(0.0).compareTo(null);
        assertThatThrownBy(() -> new TestResource(0.0).compareTo(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCompareToFailDifferentType() {
        // initialized as different anonymous classes
        final Resource resource1 = new TestResource(0.0) {};
        final Resource resource2 = new TestResource(0.0) {};
        assertThatThrownBy(() -> resource1.compareTo(resource2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCompareToFailDifferentName() {
        // initialized as different anonymous classes
        final Resource resource1 = new TestResource("name1", 0.0);
        final Resource resource2 = new TestResource("name2", 0.0);
        assertThatThrownBy(() -> resource1.compareTo(resource2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** This test assume that the scale limitation is 8. */
    @Test
    void testValueScaleLimited() {
        final Resource v1 = new TestResource(0.100000001);
        assertTestResourceValueEquals(0.1, v1);

        final Resource v2 = new TestResource(1.0).divide(3);
        assertTestResourceValueEquals(0.33333333, v2);
    }

    @Test
    void testStripTrailingZeros() {
        final Resource v = new TestResource(0.25).multiply(2);
        assertThat(v.getValue().toString()).isEqualTo("0.5");
    }

    private static void assertTestResourceValueEquals(final double value, final Resource resource) {
        assertThat(resource).isEqualTo(new TestResource(value));
    }
}
