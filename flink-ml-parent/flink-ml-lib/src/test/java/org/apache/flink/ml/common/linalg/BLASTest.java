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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * The test cases for {@link BLAS}.
 */
public class BLASTest {
	private static final double TOL = 1.0e-8;
	private DenseMatrix mat = new DenseMatrix(2, 3, new double[]{1, 4, 2, 5, 3, 6});
	private DenseVector dv1 = new DenseVector(new double[]{1, 2});
	private DenseVector dv2 = new DenseVector(new double[]{1, 2, 3});
	private SparseVector spv1 = new SparseVector(2, new int[]{0, 1}, new double[]{1, 2});
	private SparseVector spv2 = new SparseVector(3, new int[]{0, 2}, new double[]{1, 3});

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testAsum() throws Exception {
		Assert.assertEquals(BLAS.asum(dv1), 3.0, TOL);
		Assert.assertEquals(BLAS.asum(spv1), 3.0, TOL);
	}

	@Test
	public void testScal() throws Exception {
		DenseVector v1 = dv1.clone();
		BLAS.scal(0.5, v1);
		Assert.assertArrayEquals(v1.getData(), new double[]{0.5, 1.0}, TOL);

		SparseVector v2 = spv1.clone();
		BLAS.scal(0.5, v2);
		Assert.assertArrayEquals(v2.getIndices(), spv1.getIndices());
		Assert.assertArrayEquals(v2.getValues(), new double[]{0.5, 1.0}, TOL);
	}

	@Test
	public void testDot() throws Exception {
		DenseVector v = DenseVector.ones(2);
		Assert.assertEquals(BLAS.dot(dv1, v), 3.0, TOL);
	}

	@Test
	public void testAxpy() throws Exception {
		DenseVector v = DenseVector.ones(2);
		BLAS.axpy(1.0, dv1, v);
		Assert.assertArrayEquals(v.getData(), new double[]{2, 3}, TOL);
		BLAS.axpy(1.0, spv1, v);
		Assert.assertArrayEquals(v.getData(), new double[]{3, 5}, TOL);
		BLAS.axpy(1, 1.0, new double[]{1}, 0, v.getData(), 1);
		Assert.assertArrayEquals(v.getData(), new double[]{3, 6}, TOL);
	}

	private DenseMatrix simpleMM(DenseMatrix m1, DenseMatrix m2) {
		DenseMatrix mm = new DenseMatrix(m1.numRows(), m2.numCols());
		for (int i = 0; i < m1.numRows(); i++) {
			for (int j = 0; j < m2.numCols(); j++) {
				double s = 0.;
				for (int k = 0; k < m1.numCols(); k++) {
					s += m1.get(i, k) * m2.get(k, j);
				}
				mm.set(i, j, s);
			}
		}
		return mm;
	}

	@Test
	public void testGemm() throws Exception {
		DenseMatrix m32 = DenseMatrix.rand(3, 2);
		DenseMatrix m24 = DenseMatrix.rand(2, 4);
		DenseMatrix m34 = DenseMatrix.rand(3, 4);
		DenseMatrix m42 = DenseMatrix.rand(4, 2);
		DenseMatrix m43 = DenseMatrix.rand(4, 3);

		DenseMatrix a34 = DenseMatrix.zeros(3, 4);
		BLAS.gemm(1.0, m32, false, m24, false, 0., a34);
		Assert.assertArrayEquals(a34.getData(), simpleMM(m32, m24).getData(), TOL);

		BLAS.gemm(1.0, m32, false, m42, true, 0., a34);
		Assert.assertArrayEquals(a34.getData(), simpleMM(m32, m42.transpose()).getData(), TOL);

		DenseMatrix a24 = DenseMatrix.zeros(2, 4);
		BLAS.gemm(1.0, m32, true, m34, false, 0., a24);
		Assert.assertArrayEquals(a24.getData(), simpleMM(m32.transpose(), m34).getData(), TOL);

		BLAS.gemm(1.0, m32, true, m43, true, 0., a24);
		Assert.assertArrayEquals(a24.getData(), simpleMM(m32.transpose(), m43.transpose()).getData(), TOL);
	}

	@Test
	public void testGemmSizeCheck() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		DenseMatrix m32 = DenseMatrix.rand(3, 2);
		DenseMatrix m42 = DenseMatrix.rand(4, 2);
		DenseMatrix a34 = DenseMatrix.zeros(3, 4);
		BLAS.gemm(1.0, m32, false, m42, false, 0., a34);
	}

	@Test
	public void testGemmTransposeSizeCheck() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		DenseMatrix m32 = DenseMatrix.rand(3, 2);
		DenseMatrix m42 = DenseMatrix.rand(4, 2);
		DenseMatrix a34 = DenseMatrix.zeros(3, 4);
		BLAS.gemm(1.0, m32, true, m42, true, 0., a34);
	}

	@Test
	public void testGemvDense() throws Exception {
		DenseVector y1 = DenseVector.ones(2);
		BLAS.gemv(2.0, mat, false, dv2, 0., y1);
		Assert.assertArrayEquals(new double[]{28, 64}, y1.data, TOL);

		DenseVector y2 = DenseVector.ones(2);
		BLAS.gemv(2.0, mat, false, dv2, 1., y2);
		Assert.assertArrayEquals(new double[]{29, 65}, y2.data, TOL);
	}

	@Test
	public void testGemvDenseTranspose() throws Exception {
		DenseVector y1 = DenseVector.ones(3);
		BLAS.gemv(1.0, mat, true, dv1, 0., y1);
		Assert.assertArrayEquals(new double[]{9, 12, 15}, y1.data, TOL);

		DenseVector y2 = DenseVector.ones(3);
		BLAS.gemv(1.0, mat, true, dv1, 1., y2);
		Assert.assertArrayEquals(new double[]{10, 13, 16}, y2.data, TOL);
	}

	@Test
	public void testGemvSparse() throws Exception {
		DenseVector y1 = DenseVector.ones(2);
		BLAS.gemv(2.0, mat, false, spv2, 0., y1);
		Assert.assertArrayEquals(new double[]{20, 44}, y1.data, TOL);

		DenseVector y2 = DenseVector.ones(2);
		BLAS.gemv(2.0, mat, false, spv2, 1., y2);
		Assert.assertArrayEquals(new double[]{21, 45}, y2.data, TOL);
	}

	@Test
	public void testGemvSparseTranspose() throws Exception {
		DenseVector y1 = DenseVector.ones(3);
		BLAS.gemv(2.0, mat, true, spv1, 0., y1);
		Assert.assertArrayEquals(new double[]{18, 24, 30}, y1.data, TOL);

		DenseVector y2 = DenseVector.ones(3);
		BLAS.gemv(2.0, mat, true, spv1, 1., y2);
		Assert.assertArrayEquals(new double[]{19, 25, 31}, y2.data, TOL);
	}

	@Test
	public void testGemvSizeCheck() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		DenseVector y = DenseVector.ones(2);
		BLAS.gemv(2.0, mat, false, dv1, 0., y);
	}

	@Test
	public void testGemvTransposeSizeCheck() throws Exception {
		thrown.expect(IllegalArgumentException.class);
		DenseVector y = DenseVector.ones(2);
		BLAS.gemv(2.0, mat, true, dv1, 0., y);
	}
}
