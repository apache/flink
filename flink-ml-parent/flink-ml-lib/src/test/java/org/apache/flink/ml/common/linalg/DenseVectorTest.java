/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.linalg;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test cases for DenseVector. */
public class DenseVectorTest {
    private static final double TOL = 1.0e-6;

    @Test
    public void testSize() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        Assertions.assertEquals(vec.size(), 3);
    }

    @Test
    public void testNormL1() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        Assertions.assertEquals(vec.normL1(), 6, 0);
    }

    @Test
    public void testNormMax() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        Assertions.assertEquals(vec.normInf(), 3, 0);
    }

    @Test
    public void testNormL2() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        Assertions.assertEquals(vec.normL2(), Math.sqrt(1 + 4 + 9), TOL);
    }

    @Test
    public void testNormL2Square() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        Assertions.assertEquals(vec.normL2Square(), 1 + 4 + 9, TOL);
    }

    @Test
    public void testSlice() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        DenseVector sliced = vec.slice(new int[] {0, 2});
        Assertions.assertArrayEquals(new double[] {1, -3}, sliced.getData(), 0);
    }

    @Test
    public void testMinus() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        DenseVector d = new DenseVector(new double[] {1, 2, 1});
        DenseVector vec2 = vec.minus(d);
        Assertions.assertArrayEquals(vec.getData(), new double[] {1, 2, -3}, 0);
        Assertions.assertArrayEquals(vec2.getData(), new double[] {0, 0, -4}, TOL);
        vec.minusEqual(d);
        Assertions.assertArrayEquals(vec.getData(), new double[] {0, 0, -4}, TOL);
    }

    @Test
    public void testPlus() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        DenseVector d = new DenseVector(new double[] {1, 2, 1});
        DenseVector vec2 = vec.plus(d);
        Assertions.assertArrayEquals(vec.getData(), new double[] {1, 2, -3}, 0);
        Assertions.assertArrayEquals(vec2.getData(), new double[] {2, 4, -2}, TOL);
        vec.plusEqual(d);
        Assertions.assertArrayEquals(vec.getData(), new double[] {2, 4, -2}, TOL);
    }

    @Test
    public void testPlusScaleEqual() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        DenseVector vec2 = new DenseVector(new double[] {1, 0, 2});
        vec.plusScaleEqual(vec2, 2.);
        Assertions.assertArrayEquals(vec.getData(), new double[] {3, 2, 1}, TOL);
    }

    @Test
    public void testDot() throws Exception {
        DenseVector vec1 = new DenseVector(new double[] {1, 2, -3});
        DenseVector vec2 = new DenseVector(new double[] {3, 2, 1});
        Assertions.assertEquals(vec1.dot(vec2), 3 + 4 - 3, TOL);
    }

    @Test
    public void testPrefix() throws Exception {
        DenseVector vec1 = new DenseVector(new double[] {1, 2, -3});
        DenseVector vec2 = vec1.prefix(0);
        Assertions.assertArrayEquals(vec2.getData(), new double[] {0, 1, 2, -3}, 0);
    }

    @Test
    public void testAppend() throws Exception {
        DenseVector vec1 = new DenseVector(new double[] {1, 2, -3});
        DenseVector vec2 = vec1.append(0);
        Assertions.assertArrayEquals(vec2.getData(), new double[] {1, 2, -3, 0}, 0);
    }

    @Test
    public void testOuter() throws Exception {
        DenseVector vec1 = new DenseVector(new double[] {1, 2, -3});
        DenseVector vec2 = new DenseVector(new double[] {3, 2, 1});
        DenseMatrix outer = vec1.outer(vec2);
        Assertions.assertArrayEquals(
                outer.getArrayCopy1D(true), new double[] {3, 2, 1, 6, 4, 2, -9, -6, -3}, TOL);
    }

    @Test
    public void testNormalize() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        vec.normalizeEqual(1.0);
        Assertions.assertArrayEquals(vec.getData(), new double[] {1. / 6, 2. / 6, -3. / 6}, TOL);
    }

    @Test
    public void testStandardize() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        vec.standardizeEqual(1.0, 1.0);
        Assertions.assertArrayEquals(vec.getData(), new double[] {0, 1, -4}, TOL);
    }

    @Test
    public void testIterator() throws Exception {
        DenseVector vec = new DenseVector(new double[] {1, 2, -3});
        VectorIterator iterator = vec.iterator();
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals(iterator.getIndex(), 0);
        Assertions.assertEquals(iterator.getValue(), 1, 0);
        iterator.next();
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals(iterator.getIndex(), 1);
        Assertions.assertEquals(iterator.getValue(), 2, 0);
        iterator.next();
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals(iterator.getIndex(), 2);
        Assertions.assertEquals(iterator.getValue(), -3, 0);
        iterator.next();
        Assertions.assertFalse(iterator.hasNext());
    }
}
