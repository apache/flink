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

package org.apache.flink.ml.common.matrix;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for DenseVector.
 */
public class DenseVectorTest {

	@Test
	public void size() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		Assert.assertEquals(vec.size(), 3);
	}

	@Test
	public void normL1() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		Assert.assertEquals(vec.normL1(), 6, 0);
	}

	@Test
	public void normInf() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		Assert.assertEquals(vec.normInf(), 3, 0);
	}

	@Test
	public void normL2() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		Assert.assertEquals(vec.normL2(), Math.sqrt(1 + 4 + 9), 1e-6);
	}

	@Test
	public void normL2Square() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		Assert.assertEquals(vec.normL2Square(), 1 + 4 + 9, 1e-6);
	}

	@Test
	public void slice() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		DenseVector sliced = vec.slice(new int[] {0, 2});
		Assert.assertArrayEquals(new double[] {1, -3}, sliced.getData(), 0);
	}

	@Test
	public void minus() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		DenseVector d = new DenseVector(new double[] {1, 2, 1});
		DenseVector vec2 = vec.minus(d);
		Assert.assertArrayEquals(vec.getData(), new double[] {1, 2, -3}, 0);
		Assert.assertArrayEquals(vec2.getData(), new double[] {0, 0, -4}, 1e-6);
		vec.minusEqual(d);
		Assert.assertArrayEquals(vec.getData(), new double[] {0, 0, -4}, 1e-6);
		vec.minusEqual(1.0);
		Assert.assertArrayEquals(vec.getData(), new double[] {-1, -1, -5}, 1e-6);
		DenseVector vec3 = DenseVector.zeros(3);
		vec3.minusEqual(vec, d);
		Assert.assertArrayEquals(vec3.getData(), new double[] {-2, -3, -6}, 1e-6);
	}

	@Test
	public void plus() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		DenseVector d = new DenseVector(new double[] {1, 2, 1});
		DenseVector vec2 = vec.plus(d);
		Assert.assertArrayEquals(vec.getData(), new double[] {1, 2, -3}, 0);
		Assert.assertArrayEquals(vec2.getData(), new double[] {2, 4, -2}, 1e-6);
		vec.plusEqual(d);
		Assert.assertArrayEquals(vec.getData(), new double[] {2, 4, -2}, 1e-6);
		vec.plusEqual(1.0);
		Assert.assertArrayEquals(vec.getData(), new double[] {3, 5, -1}, 1e-6);
		DenseVector vec3 = DenseVector.zeros(3);
		vec3.plusEqual(vec, d);
		Assert.assertArrayEquals(vec3.getData(), new double[] {4, 7, 0}, 1e-6);
	}

	@Test
	public void plusScaleEqual() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		SparseVector vec2 = new SparseVector(3, new int[] {0, 2}, new double[] {1, 2});
		DenseVector vec3 = new DenseVector(new double[] {1, 0, 2});
		vec.plusScaleEqual(vec2, 2.);
		Assert.assertArrayEquals(vec.getData(), new double[] {3, 2, 1}, 1e-6);
		vec.plusScaleEqual(vec3, 2.);
		Assert.assertArrayEquals(vec.getData(), new double[] {5, 2, 5}, 1e-6);
	}

	@Test
	public void dot() throws Exception {
		DenseVector vec1 = new DenseVector(new double[] {1, 2, -3});
		DenseVector vec2 = new DenseVector(new double[] {3, 2, 1});
		Assert.assertEquals(vec1.dot(vec2), 3 + 4 - 3, 1e-6);
	}

	@Test
	public void prefix() throws Exception {
		DenseVector vec1 = new DenseVector(new double[] {1, 2, -3});
		DenseVector vec2 = vec1.prefix(0);
		Assert.assertArrayEquals(vec2.getData(), new double[] {0, 1, 2, -3}, 0);
	}

	@Test
	public void append() throws Exception {
		DenseVector vec1 = new DenseVector(new double[] {1, 2, -3});
		DenseVector vec2 = vec1.append(0);
		Assert.assertArrayEquals(vec2.getData(), new double[] {1, 2, -3, 0}, 0);
	}

	@Test
	public void outer() throws Exception {
		DenseVector vec1 = new DenseVector(new double[] {1, 2, -3});
		DenseVector vec2 = new DenseVector(new double[] {3, 2, 1});
		DenseMatrix outer = vec1.outer(vec2);
		Assert.assertArrayEquals(outer.getArrayCopy1D(true),
			new double[] {3, 2, 1, 6, 4, 2, -9, -6, -3}, 1e-6);
	}

	@Test
	public void serialize() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		Assert.assertEquals(vec.serialize(), "1.0,2.0,-3.0");
	}

	@Test
	public void deserialize() throws Exception {
		DenseVector vec1 = DenseVector.deserialize("1,2,-3");
		DenseVector vec2 = DenseVector.deserialize(" 1, 2, -3 ");
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		Assert.assertArrayEquals(vec1.getData(), vec.getData(), 0);
		Assert.assertArrayEquals(vec2.getData(), vec.getData(), 0);
	}

	@Test
	public void normalize() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		Vector normalized = vec.normalize(1.0);
		Assert.assertTrue(normalized instanceof DenseVector);
		Assert.assertArrayEquals(((DenseVector) normalized).getData(), new double[] {1. / 6, 2. / 6, -3. / 6}, 1e-6);
	}

	@Test
	public void iterator() throws Exception {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		VectorIterator iterator = vec.iterator();
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(iterator.getIndex(), 0);
		Assert.assertEquals(iterator.getValue(), 1, 0);
		iterator.next();
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(iterator.getIndex(), 1);
		Assert.assertEquals(iterator.getValue(), 2, 0);
		iterator.next();
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(iterator.getIndex(), 2);
		Assert.assertEquals(iterator.getValue(), -3, 0);
		iterator.next();
		Assert.assertFalse(iterator.hasNext());
	}

}
