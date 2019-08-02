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
	public void testPlus() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		DenseMatrix matB = DenseMatrix.ones(4, 3);
		matA.plusEquals(matB);
		matA.plusEquals(3.0);
	}

	@Test
	public void testMinus() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		DenseMatrix matB = DenseMatrix.ones(4, 3);
		matA.minusEquals(matB);
	}

	@Test
	public void testMatrixTimesMatrix() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		DenseMatrix matB = DenseMatrix.rand(3, 5);

		DenseMatrix matC1 = matA.times(matB);
		assertEqual2D(matC1.getArrayCopy2D(), simpleMM(matA.getArrayCopy2D(), matB.getArrayCopy2D()));

		DenseMatrix matC2 = matA.times(matB);
		assertEqual2D(matC2.getArrayCopy2D(), simpleMM(matA.getArrayCopy2D(), matB.getArrayCopy2D()));

		DenseMatrix matC3 = DenseMatrix.zeros(5, 4);
		BLAS.gemm(1., matB, true, matA, true, 0., matC3);
		assertEqual2D(matC3.getArrayCopy2D(),
			simpleMM(matB.transpose().getArrayCopy2D(), matA.transpose().getArrayCopy2D()));

		DenseMatrix matC4 = DenseMatrix.zeros(5, 4);
		BLAS.gemm(1., matB, true, matA, true, 0., matC4);
		assertEqual2D(matC4.getArrayCopy2D(),
			simpleMM(matB.transpose().getArrayCopy2D(), matA.transpose().getArrayCopy2D()));

		DenseMatrix matC5 = matA.times(matB);
		assertEqual2D(matC5.getArrayCopy2D(), simpleMM(matA.getArrayCopy2D(), matB.getArrayCopy2D()));
	}

	@Test
	public void testMatrixTimesVector() throws Exception {

		DenseMatrix matA = DenseMatrix.rand(4, 3);

		DenseVector x = DenseVector.ones(3);
		DenseVector y = matA.times(x);
		Assert.assertArrayEquals(y.getData(), simpleMV(matA.getArrayCopy2D(), x.getData()), TOL);

		DenseVector y2 = matA.times(x);
		Assert.assertArrayEquals(y2.getData(), simpleMV(matA.getArrayCopy2D(), x.getData()), TOL);

		x = DenseVector.ones(4);
		DenseVector y3 = new DenseVector(3);
		BLAS.gemv(1., matA, true, x, 0., y3);
		Assert.assertArrayEquals(y3.getData(), simpleMV(matA.transpose().getArrayCopy2D(), x.getData()), TOL);

		BLAS.gemv(1., matA, true, x, 0., y3);
		Assert.assertArrayEquals(y3.getData(), simpleMV(matA.transpose().getArrayCopy2D(), x.getData()), TOL);
	}

	@Test
	public void testDataSelection() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		double[][] matACopy = matA.getArrayCopy2D();
		DenseMatrix subA = matA.selectRows(new int[]{1});
		double[][] subACopy = subA.getArrayCopy2D();
		Assert.assertArrayEquals(matACopy[1], subACopy[0], TOL);

		DenseVector row = DenseVector.rand(3);
		matA.setRowData(row.getData(), 2);
		double[] row0 = matA.getRow(2);
		Assert.assertArrayEquals(row0, row.getData(), TOL);

		DenseVector col = DenseVector.rand(4);
		matA.setColData(col.getData(), 2);
		double[] col0 = matA.getColumn(2);
		Assert.assertArrayEquals(col0, col.getData(), TOL);
	}

	@Test
	public void testSum() throws Exception {
		DenseMatrix matA = DenseMatrix.ones(3, 2);
		Assert.assertEquals(matA.sum(), 6.0, TOL);
		Assert.assertEquals(matA.sumAbs(), 6.0, TOL);
		Assert.assertEquals(matA.sumSquare(), 6.0, TOL);
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
