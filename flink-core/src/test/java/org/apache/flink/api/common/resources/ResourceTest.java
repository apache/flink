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

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link Resource}. */
public class ResourceTest extends TestLogger {

    @Test
    public void testConstructorValid() {
        final Resource v1 = new TestResource(0.1);
        assertTestResourceValueEquals(0.1, v1);

        final Resource v2 = new TestResource(BigDecimal.valueOf(0.1));
        assertTestResourceValueEquals(0.1, v2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorInvalidValue() {
        new TestResource(-0.1);
    }

    @Test
    public void testEquals() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new TestResource(0.1);
        final Resource v3 = new TestResource(0.2);
        assertTrue(v1.equals(v2));
        assertFalse(v1.equals(v3));
    }

    @Test
    public void testEqualsIgnoringScale() {
        final Resource v1 = new TestResource(new BigDecimal("0.1"));
        final Resource v2 = new TestResource(new BigDecimal("0.10"));
        assertTrue(v1.equals(v2));
    }

    @Test
    public void testHashCodeIgnoringScale() {
        final Resource v1 = new TestResource(new BigDecimal("0.1"));
        final Resource v2 = new TestResource(new BigDecimal("0.10"));
        assertTrue(v1.hashCode() == v2.hashCode());
    }

    @Test
    public void testMerge() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new TestResource(0.2);
        assertTestResourceValueEquals(0.3, v1.merge(v2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeErrorOnDifferentTypes() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new CPUResource(0.1);
        v1.merge(v2);
    }

    @Test
    public void testSubtract() {
        final Resource v1 = new TestResource(0.2);
        final Resource v2 = new TestResource(0.1);
        assertTestResourceValueEquals(0.1, v1.subtract(v2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubtractLargerValue() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new TestResource(0.2);
        v1.subtract(v2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubtractErrorOnDifferentTypes() {
        final Resource v1 = new TestResource(0.1);
        final Resource v2 = new CPUResource(0.1);
        v1.subtract(v2);
    }

    @Test
    public void testDivide() {
        final Resource resource = new TestResource(0.04);
        final BigDecimal by = BigDecimal.valueOf(0.1);
        assertTestResourceValueEquals(0.4, resource.divide(by));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDivideNegative() {
        final Resource resource = new TestResource(1.2);
        final BigDecimal by = BigDecimal.valueOf(-0.5);
        resource.divide(by);
    }

    @Test
    public void testDivideInteger() {
        final Resource resource = new TestResource(0.12);
        final int by = 4;
        assertTestResourceValueEquals(0.03, resource.divide(by));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDivideNegativeInteger() {
        final Resource resource = new TestResource(1.2);
        final int by = -5;
        resource.divide(by);
    }

    @Test
    public void testMultiply() {
        final Resource resource = new TestResource(0.3);
        final BigDecimal by = BigDecimal.valueOf(0.2);
        assertTestResourceValueEquals(0.06, resource.multiply(by));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMutiplyNegative() {
        final Resource resource = new TestResource(0.3);
        final BigDecimal by = BigDecimal.valueOf(-0.2);
        resource.multiply(by);
    }

    @Test
    public void testMultiplyInteger() {
        final Resource resource = new TestResource(0.3);
        final int by = 2;
        assertTestResourceValueEquals(0.6, resource.multiply(by));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMutiplyNegativeInteger() {
        final Resource resource = new TestResource(0.3);
        final int by = -2;
        resource.multiply(by);
    }

    @Test
    public void testIsZero() {
        final Resource resource1 = new TestResource(0.0);
        final Resource resource2 = new TestResource(1.0);

        assertTrue(resource1.isZero());
        assertFalse(resource2.isZero());
    }

    @Test
    public void testCompareTo() {
        final Resource resource1 = new TestResource(0.0);
        final Resource resource2 = new TestResource(0.0);
        final Resource resource3 = new TestResource(1.0);

        assertThat(resource1.compareTo(resource1), is(0));
        assertThat(resource1.compareTo(resource2), is(0));
        assertThat(resource1.compareTo(resource3), lessThan(0));
        assertThat(resource3.compareTo(resource1), greaterThan(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompareToFailNull() {
        new TestResource(0.0).compareTo(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompareToFailDifferentType() {
        // initialized as different anonymous classes
        final Resource resource1 = new TestResource(0.0) {};
        final Resource resource2 = new TestResource(0.0) {};
        resource1.compareTo(resource2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompareToFailDifferentName() {
        // initialized as different anonymous classes
        final Resource resource1 = new TestResource("name1", 0.0);
        final Resource resource2 = new TestResource("name2", 0.0);
        resource1.compareTo(resource2);
    }

    private static void assertTestResourceValueEquals(final double value, final Resource resource) {
        assertEquals(new TestResource(value), resource);
    }
}
