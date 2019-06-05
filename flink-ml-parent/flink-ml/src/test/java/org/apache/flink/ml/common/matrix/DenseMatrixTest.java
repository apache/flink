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
 * Test for DenseMatrix.
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
	public void plus() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		DenseMatrix matB = DenseMatrix.ones(4, 3);
		matA.plusEquals(matB);
		matA.plusEquals(3.0);
	}

	@Test
	public void minus() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		DenseMatrix matB = DenseMatrix.ones(4, 3);
		matA.minusEquals(matB);
	}

	@Test
	public void selectData() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		double[][] matACopy = matA.getArrayCopy2D();
		DenseMatrix subA = matA.selectRows(new int[] {1});
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
	public void sum() throws Exception {
		DenseMatrix matA = DenseMatrix.ones(3, 2);
		Assert.assertEquals(matA.sum(), 6.0, TOL);
		Assert.assertEquals(matA.sumAbs(), 6.0, TOL);
		Assert.assertEquals(matA.sumSquare(), 6.0, TOL);
	}

	@Test
	public void changeMajor() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		double[][] dA = matA.getArrayCopy2D();
		double[][] dA2 = matA.getArrayCopy2D();
		assertEqual2D(dA, dA2);
	}
}
