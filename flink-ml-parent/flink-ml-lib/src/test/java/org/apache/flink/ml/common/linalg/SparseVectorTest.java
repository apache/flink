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

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

/** Test cases for SparseVector. */
public class SparseVectorTest {
    private static final double TOL = 1.0e-6;
    private SparseVector v1 =
            new SparseVector(8, new int[] {1, 3, 5, 7}, new double[] {2.0, 2.0, 2.0, 2.0});
    private SparseVector v2 =
            new SparseVector(8, new int[] {3, 4, 5}, new double[] {1.0, 1.0, 1.0});

    @Test
    public void testConstructor() throws Exception {
        int[] indices = new int[] {3, 7, 2, 1};
        double[] values = new double[] {3.0, 7.0, 2.0, 1.0};
        Map<Integer, Double> map = new TreeMap<>();
        for (int i = 0; i < indices.length; i++) {
            map.put(indices[i], values[i]);
        }
        SparseVector v = new SparseVector(8, map);
        Assert.assertArrayEquals(v.getIndices(), new int[] {1, 2, 3, 7});
        Assert.assertArrayEquals(v.getValues(), new double[] {1, 2, 3, 7}, TOL);
    }

    @Test
    public void testSize() throws Exception {
        Assert.assertEquals(v1.size(), 8);
    }

    @Test
    public void testSet() throws Exception {
        SparseVector v = v1.clone();
        v.set(2, 2.0);
        v.set(3, 3.0);
        Assert.assertEquals(v.get(2), 2.0, TOL);
        Assert.assertEquals(v.get(3), 3.0, TOL);
    }

    @Test
    public void testAdd() throws Exception {
        SparseVector v = v1.clone();
        v.add(2, 2.0);
        v.add(3, 3.0);
        Assert.assertEquals(v.get(2), 2.0, TOL);
        Assert.assertEquals(v.get(3), 5.0, TOL);
    }

    @Test
    public void testPrefix() throws Exception {
        SparseVector prefixed = v1.prefix(0.2);
        Assert.assertArrayEquals(prefixed.getIndices(), new int[] {0, 2, 4, 6, 8});
        Assert.assertArrayEquals(prefixed.getValues(), new double[] {0.2, 2, 2, 2, 2}, 0);
    }

    @Test
    public void testAppend() throws Exception {
        SparseVector prefixed = v1.append(0.2);
        Assert.assertArrayEquals(prefixed.getIndices(), new int[] {1, 3, 5, 7, 8});
        Assert.assertArrayEquals(prefixed.getValues(), new double[] {2, 2, 2, 2, 0.2}, 0);
    }

    @Test
    public void testSortIndices() throws Exception {
        int n = 8;
        int[] indices = new int[] {7, 5, 3, 1};
        double[] values = new double[] {7, 5, 3, 1};
        v1 = new SparseVector(n, indices, values);
        Assert.assertArrayEquals(values, new double[] {1, 3, 5, 7}, 0.);
        Assert.assertArrayEquals(v1.getValues(), new double[] {1, 3, 5, 7}, 0.);
        Assert.assertArrayEquals(indices, new int[] {1, 3, 5, 7});
        Assert.assertArrayEquals(v1.getIndices(), new int[] {1, 3, 5, 7});
    }

    @Test
    public void testNormL2Square() throws Exception {
        Assert.assertEquals(v2.normL2Square(), 3.0, TOL);
    }

    @Test
    public void testMinus() throws Exception {
        Vector d = v2.minus(v1);
        Assert.assertEquals(d.get(0), 0.0, TOL);
        Assert.assertEquals(d.get(1), -2.0, TOL);
        Assert.assertEquals(d.get(2), 0.0, TOL);
        Assert.assertEquals(d.get(3), -1.0, TOL);
        Assert.assertEquals(d.get(4), 1.0, TOL);
    }

    @Test
    public void testPlus() throws Exception {
        Vector d = v1.plus(v2);
        Assert.assertEquals(d.get(0), 0.0, TOL);
        Assert.assertEquals(d.get(1), 2.0, TOL);
        Assert.assertEquals(d.get(2), 0.0, TOL);
        Assert.assertEquals(d.get(3), 3.0, TOL);

        DenseVector dv = DenseVector.ones(8);
        dv = dv.plus(v2);
        Assert.assertArrayEquals(dv.getData(), new double[] {1, 1, 1, 2, 2, 2, 1, 1}, TOL);
    }

    @Test
    public void testDot() throws Exception {
        Assert.assertEquals(v1.dot(v2), 4.0, TOL);
    }

    @Test
    public void testGet() throws Exception {
        Assert.assertEquals(v1.get(5), 2.0, TOL);
        Assert.assertEquals(v1.get(6), 0.0, TOL);
    }

    @Test
    public void testSlice() throws Exception {
        int n = 8;
        int[] indices = new int[] {1, 3, 5, 7};
        double[] values = new double[] {2.0, 3.0, 4.0, 5.0};
        SparseVector v = new SparseVector(n, indices, values);

        int[] indices1 = new int[] {5, 4, 3};
        SparseVector vec1 = v.slice(indices1);
        Assert.assertEquals(vec1.size(), 3);
        Assert.assertArrayEquals(vec1.getIndices(), new int[] {0, 2});
        Assert.assertArrayEquals(vec1.getValues(), new double[] {4.0, 3.0}, 0.0);

        int[] indices2 = new int[] {3, 5};
        SparseVector vec2 = v.slice(indices2);
        Assert.assertArrayEquals(vec2.getIndices(), new int[] {0, 1});
        Assert.assertArrayEquals(vec2.getValues(), new double[] {3.0, 4.0}, 0.0);

        int[] indices3 = new int[] {2, 4};
        SparseVector vec3 = v.slice(indices3);
        Assert.assertEquals(vec3.size(), 2);
        Assert.assertArrayEquals(vec3.getIndices(), new int[] {});
        Assert.assertArrayEquals(vec3.getValues(), new double[] {}, 0.0);

        int[] indices4 = new int[] {2, 2, 4, 4};
        SparseVector vec4 = v.slice(indices4);
        Assert.assertEquals(vec4.size(), 4);
        Assert.assertArrayEquals(vec4.getIndices(), new int[] {});
        Assert.assertArrayEquals(vec4.getValues(), new double[] {}, 0.0);
    }

    @Test
    public void testToDenseVector() throws Exception {
        int[] indices = new int[] {1, 3, 5};
        double[] values = new double[] {1.0, 3.0, 5.0};
        SparseVector v = new SparseVector(-1, indices, values);
        DenseVector dv = v.toDenseVector();
        Assert.assertEquals(dv.size(), 6);
        Assert.assertArrayEquals(dv.getData(), new double[] {0, 1, 0, 3, 0, 5}, TOL);
    }

    @Test
    public void testRemoveZeroValues() throws Exception {
        int[] indices = new int[] {1, 3, 5};
        double[] values = new double[] {0.0, 3.0, 0.0};
        SparseVector v = new SparseVector(6, indices, values);
        v.removeZeroValues();
        Assert.assertArrayEquals(v.getIndices(), new int[] {3});
        Assert.assertArrayEquals(v.getValues(), new double[] {3}, TOL);
    }

    @Test
    public void testOuter() throws Exception {
        DenseMatrix outerProduct = v1.outer(v2);
        Assert.assertEquals(outerProduct.numRows(), 8);
        Assert.assertEquals(outerProduct.numCols(), 8);
        Assert.assertArrayEquals(
                outerProduct.getRow(0), new double[] {0, 0, 0, 0, 0, 0, 0, 0}, TOL);
        Assert.assertArrayEquals(
                outerProduct.getRow(1), new double[] {0, 0, 0, 2, 2, 2, 0, 0}, TOL);
        Assert.assertArrayEquals(
                outerProduct.getRow(2), new double[] {0, 0, 0, 0, 0, 0, 0, 0}, TOL);
        Assert.assertArrayEquals(
                outerProduct.getRow(3), new double[] {0, 0, 0, 2, 2, 2, 0, 0}, TOL);
        Assert.assertArrayEquals(
                outerProduct.getRow(4), new double[] {0, 0, 0, 0, 0, 0, 0, 0}, TOL);
        Assert.assertArrayEquals(
                outerProduct.getRow(5), new double[] {0, 0, 0, 2, 2, 2, 0, 0}, TOL);
        Assert.assertArrayEquals(
                outerProduct.getRow(6), new double[] {0, 0, 0, 0, 0, 0, 0, 0}, TOL);
        Assert.assertArrayEquals(
                outerProduct.getRow(7), new double[] {0, 0, 0, 2, 2, 2, 0, 0}, TOL);
    }

    @Test
    public void testIterator() throws Exception {
        VectorIterator iterator = v1.iterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(iterator.getIndex(), 1);
        Assert.assertEquals(iterator.getValue(), 2, 0);
        iterator.next();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(iterator.getIndex(), 3);
        Assert.assertEquals(iterator.getValue(), 2, 0);
        iterator.next();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(iterator.getIndex(), 5);
        Assert.assertEquals(iterator.getValue(), 2, 0);
        iterator.next();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(iterator.getIndex(), 7);
        Assert.assertEquals(iterator.getValue(), 2, 0);
        iterator.next();
        Assert.assertFalse(iterator.hasNext());
    }
}
