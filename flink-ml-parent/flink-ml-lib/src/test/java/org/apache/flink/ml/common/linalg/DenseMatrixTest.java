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
 * Test cases for DenseMatrix.
 */
public class DenseMatrixTest {

	private static final double TOL = 1.0e-6;

	private static void assertEqual2D(double[][] matA, double[][] matB) {
		assert (matA.length == matB.length);
		assert (matA[0].length == matB[0].length);
		int m = matA.length;
		int n = matA[0].length;
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				Assert.assertEquals(matA[i][j], matB[i][j], TOL);
			}
		}
	}

	private static double[][] simpleMM(double[][] matA, double[][] matB) {
		int m = matA.length;
		int n = matB[0].length;
		int k = matA[0].length;
		double[][] matC = new double[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				matC[i][j] = 0.;
				for (int l = 0; l < k; l++) {
					matC[i][j] += matA[i][l] * matB[l][j];
				}
			}
		}
		return matC;
	}

	private static double[] simpleMV(double[][] matA, double[] x) {
		int m = matA.length;
		int n = matA[0].length;
		assert (n == x.length);
		double[] y = new double[m];
		for (int i = 0; i < m; i++) {
			y[i] = 0.;
			for (int j = 0; j < n; j++) {
				y[i] += matA[i][j] * x[j];
			}
		}
		return y;
	}

	@Test
	public void testPlusEquals() throws Exception {
		DenseMatrix matA = new DenseMatrix(new double[][]{
			new double[]{1, 3, 5},
			new double[]{2, 4, 6},
		});
		DenseMatrix matB = DenseMatrix.ones(2, 3);
		matA.plusEquals(matB);
		Assert.assertArrayEquals(matA.getData(), new double[]{2, 3, 4, 5, 6, 7}, TOL);
		matA.plusEquals(1.0);
		Assert.assertArrayEquals(matA.getData(), new double[]{3, 4, 5, 6, 7, 8}, TOL);
	}

	@Test
	public void testMinusEquals() throws Exception {
		DenseMatrix matA = new DenseMatrix(new double[][]{
			new double[]{1, 3, 5},
			new double[]{2, 4, 6},
		});
		DenseMatrix matB = DenseMatrix.ones(2, 3);
		matA.minusEquals(matB);
		Assert.assertArrayEquals(matA.getData(), new double[]{0, 1, 2, 3, 4, 5}, TOL);
	}

	@Test
	public void testPlus() throws Exception {
		DenseMatrix matA = new DenseMatrix(new double[][]{
			new double[]{1, 3, 5},
			new double[]{2, 4, 6},
		});
		DenseMatrix matB = DenseMatrix.ones(2, 3);
		DenseMatrix matC = matA.plus(matB);
		Assert.assertArrayEquals(matC.getData(), new double[]{2, 3, 4, 5, 6, 7}, TOL);
		DenseMatrix matD = matA.plus(1.0);
		Assert.assertArrayEquals(matD.getData(), new double[]{2, 3, 4, 5, 6, 7}, TOL);
	}

	@Test
	public void testMinus() throws Exception {
		DenseMatrix matA = new DenseMatrix(new double[][]{
			new double[]{1, 3, 5},
			new double[]{2, 4, 6},
		});
		DenseMatrix matB = DenseMatrix.ones(2, 3);
		DenseMatrix matC = matA.minus(matB);
		Assert.assertArrayEquals(matC.getData(), new double[]{0, 1, 2, 3, 4, 5}, TOL);
	}

	@Test
	public void testMM() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		DenseMatrix matB = DenseMatrix.rand(3, 5);
		DenseMatrix matC = matA.multiplies(matB);
		assertEqual2D(matC.getArrayCopy2D(), simpleMM(matA.getArrayCopy2D(), matB.getArrayCopy2D()));

		DenseMatrix matD = new DenseMatrix(5, 4);
		BLAS.gemm(1., matB, true, matA, true, 0., matD);
		Assert.assertArrayEquals(matD.transpose().getData(), matC.data, TOL);
	}

	@Test
	public void testMV() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		DenseVector x = DenseVector.ones(3);
		DenseVector y = matA.multiplies(x);
		Assert.assertArrayEquals(y.getData(), simpleMV(matA.getArrayCopy2D(), x.getData()), TOL);

		SparseVector x2 = new SparseVector(3, new int[]{0, 1, 2}, new double[]{1, 1, 1});
		DenseVector y2 = matA.multiplies(x2);
		Assert.assertArrayEquals(y2.getData(), y.getData(), TOL);
	}

	@Test
	public void testDataSelection() throws Exception {
		DenseMatrix mat = new DenseMatrix(new double[][]{
			new double[]{1, 2, 3},
			new double[]{4, 5, 6},
			new double[]{7, 8, 9},
		});
		DenseMatrix sub1 = mat.selectRows(new int[]{1});
		DenseMatrix sub2 = mat.getSubMatrix(1, 2, 1, 2);
		Assert.assertEquals(sub1.numRows(), 1);
		Assert.assertEquals(sub1.numCols(), 3);
		Assert.assertEquals(sub2.numRows(), 1);
		Assert.assertEquals(sub2.numCols(), 1);
		Assert.assertArrayEquals(sub1.getData(), new double[]{4, 5, 6}, TOL);
		Assert.assertArrayEquals(sub2.getData(), new double[]{5}, TOL);

		double[] row = mat.getRow(1);
		double[] col = mat.getColumn(1);
		Assert.assertArrayEquals(row, new double[]{4, 5, 6}, 0.);
		Assert.assertArrayEquals(col, new double[]{2, 5, 8}, 0.);
	}

	@Test
	public void testSum() throws Exception {
		DenseMatrix matA = DenseMatrix.ones(3, 2);
		Assert.assertEquals(matA.sum(), 6.0, TOL);
	}

	@Test
	public void testRowMajorFormat() throws Exception {
		double[] data = new double[]{1, 2, 3, 4, 5, 6};
		DenseMatrix matA = new DenseMatrix(2, 3, data, true);
		Assert.assertArrayEquals(data, new double[]{1, 4, 2, 5, 3, 6}, 0.);
		Assert.assertArrayEquals(matA.getData(), new double[]{1, 4, 2, 5, 3, 6}, 0.);

		data = new double[]{1, 2, 3, 4};
		matA = new DenseMatrix(2, 2, data, true);
		Assert.assertArrayEquals(data, new double[]{1, 3, 2, 4}, 0.);
		Assert.assertArrayEquals(matA.getData(), new double[]{1, 3, 2, 4}, 0.);
	}
}
