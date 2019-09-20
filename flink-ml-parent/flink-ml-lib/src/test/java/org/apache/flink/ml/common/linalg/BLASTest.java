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
}
