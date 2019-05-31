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
	public void norm2() throws Exception {
		DenseMatrix matA = DenseMatrix.zeros(4, 5);
		matA.set(0, 0, 1);
		matA.set(0, 4, 2);
		matA.set(1, 2, 3);
		matA.set(3, 1, 2);
		Assert.assertEquals(matA.norm2(), 3., TOL);
	}

	@Test
	public void cond() throws Exception {
		DenseMatrix matA = DenseMatrix.zeros(4, 5);
		matA.set(0, 0, 1);
		matA.set(0, 4, 2);
		matA.set(1, 2, 3);
		matA.set(3, 1, 2);
		double[] answer = new double[] {3.0, 2.23606797749979, 2.0, 0.0};
		Assert.assertArrayEquals(answer, new SingularValueDecomposition(matA).getSingularValues().getData(), TOL);
	}

	@Test
	public void det() throws Exception {
		double[][] data1 = {
			{-2, 2, -3},
			{-1, 1, 3},
			{2, 0, -1},
		};
		DenseMatrix matA = new DenseMatrix(data1);
		Assert.assertEquals(matA.det(), 18.0, TOL);
	}

	@Test
	public void rank() throws Exception {
		double[][] data1 = {
			{-2, 2, -3},
			{-1, 1, 3},
			{2, 0, -1},
		};
		DenseMatrix matA = new DenseMatrix(data1);
		Assert.assertEquals(matA.rank(), 3);

		double[][] data2 = {
			{-2, 2, -3},
			{-1, 1, -1.5},
			{2, 0, -1},
			{2, 0, -1},
		};
		DenseMatrix matB = new DenseMatrix(data2);
		Assert.assertEquals(matB.rank(), 2);
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
	public void times() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		DenseMatrix matB = DenseMatrix.rand(3, 5);

		DenseMatrix matC1 = matA.times(matB);
		assertEqual2D(matC1.getArrayCopy2D(), simpleMM(matA.getArrayCopy2D(), matB.getArrayCopy2D()));

		DenseMatrix matC2 = matA.times(matB);
		assertEqual2D(matC2.getArrayCopy2D(), simpleMM(matA.getArrayCopy2D(), matB.getArrayCopy2D()));

		DenseMatrix matC3 = DenseMatrix.zeros(5, 4);
		MatVecOp.gemm(1., matB, true, matA, true, 0., matC3);
		assertEqual2D(matC3.getArrayCopy2D(),
			simpleMM(matB.transpose().getArrayCopy2D(), matA.transpose().getArrayCopy2D()));

		DenseMatrix matC4 = DenseMatrix.zeros(5, 4);
		MatVecOp.gemm(1., matB, true, matA, true, 0., matC4);
		assertEqual2D(matC4.getArrayCopy2D(),
			simpleMM(matB.transpose().getArrayCopy2D(), matA.transpose().getArrayCopy2D()));

		DenseMatrix matC5 = matA.times(matB);
		assertEqual2D(matC5.getArrayCopy2D(), simpleMM(matA.getArrayCopy2D(), matB.getArrayCopy2D()));
	}

	@Test
	public void times1() throws Exception {

		DenseMatrix matA = DenseMatrix.rand(4, 3);

		DenseVector x = DenseVector.ones(3);
		DenseVector y = matA.times(x);
		Assert.assertArrayEquals(y.getData(), simpleMV(matA.getArrayCopy2D(), x.getData()), TOL);

		DenseVector y2 = matA.times(x);
		Assert.assertArrayEquals(y2.getData(), simpleMV(matA.getArrayCopy2D(), x.getData()), TOL);

		x = DenseVector.ones(4);
		DenseVector y3 = new DenseVector(3);
		MatVecOp.gemv(1., matA, true, x, 0., y3);
		Assert.assertArrayEquals(y3.getData(), simpleMV(matA.transpose().getArrayCopy2D(), x.getData()), TOL);

		MatVecOp.gemv(1., matA, true, x, 0., y3);
		Assert.assertArrayEquals(y3.getData(), simpleMV(matA.transpose().getArrayCopy2D(), x.getData()), TOL);
	}

	@Test
	public void times2() throws Exception {
		{
			DenseMatrix matA = DenseMatrix.rand(4, 3);
			DenseVector x = DenseVector.ones(4);
			DenseVector y = DenseVector.zeros(3);
			MatVecOp.gemv(1.0, matA, true, x, 0., y);
			Assert.assertArrayEquals(y.getData(), simpleMV(matA.transpose().getArrayCopy2D(), x.getData()), TOL);
		}

		{
			DenseMatrix matA = DenseMatrix.rand(4, 3);
			DenseVector x = DenseVector.ones(4);
			DenseVector y = DenseVector.zeros(3);
			MatVecOp.gemv(1.0, matA, true, x, 0., y);
			Assert.assertArrayEquals(y.getData(), simpleMV(matA.transpose().getArrayCopy2D(), x.getData()), TOL);
		}

		{
			DenseMatrix matA = DenseMatrix.rand(4, 3);
			DenseVector x = DenseVector.ones(3);
			DenseVector y = DenseVector.zeros(4);
			MatVecOp.gemv(1.0, matA, false, x, 0., y);
			Assert.assertArrayEquals(y.getData(), simpleMV(matA.getArrayCopy2D(), x.getData()), TOL);
		}
	}

	@Test
	public void solve() throws Exception {
		{
			// symmetric case
			DenseMatrix matA = DenseMatrix.randSymmetric(5);
			DenseMatrix b = DenseMatrix.rand(5, 7);
			DenseMatrix x = matA.solve(b);
			DenseMatrix b0 = matA.times(x);
			assertEqual2D(b.getArrayCopy2D(), b0.getArrayCopy2D());
		}

		{
			// non-symmetric case
			DenseMatrix matA = DenseMatrix.rand(5, 5);
			DenseMatrix b = DenseMatrix.rand(5, 7);
			DenseMatrix x = matA.solve(b);
			DenseMatrix b0 = matA.times(x);
			assertEqual2D(b.getArrayCopy2D(), b0.getArrayCopy2D());
		}

		{
			// under determined case
			DenseMatrix matA = DenseMatrix.rand(3, 5);
			DenseMatrix b = DenseMatrix.rand(3, 7);
			DenseMatrix x = matA.solve(b);
			DenseMatrix b0 = matA.times(x);
			assertEqual2D(b.getArrayCopy2D(), b0.getArrayCopy2D());
		}

		{
			// over determined case
			DenseMatrix matA = DenseMatrix.rand(5, 3);
			DenseVector b = DenseVector.rand(5);
			DenseVector x = matA.solve(b);
			DenseVector r = matA.times(x).minus(b);
			double nr = r.normL2Square();

			for (int i = 0; i < 10; i++) {
				DenseVector x0 = DenseVector.rand(3);
				double nr0 = matA.times(x0).minus(b).normL2();
				Assert.assertTrue(nr <= nr0);
			}
		}
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
	public void solveLS() throws Exception {
		double[][] data = {
			{-2, 2, -3},
			{-1, 1, -1.5},
			{2, 0, -1},
			{2, 0, -1},
		};
		DenseMatrix matA = new DenseMatrix(data);
		DenseMatrix matB = DenseMatrix.rand(4, 1);
		DenseMatrix matX = matA.solveLS(matB);

		DenseMatrix r = matA.times(matX).minus(matB);

		DenseVector b = DenseVector.rand(4);
		DenseVector x = matA.solveLS(b);
	}

	@Test
	public void solveEigen() throws Exception {
		DenseMatrix matA = DenseMatrix.randSymmetric(5);
		scala.Tuple2 <DenseVector, DenseMatrix> result = EigenSolver.solve(matA, 3, 0.01, 300);
		DenseVector evs = result._1;
		DenseMatrix evec = result._2;
	}

	@Test
	public void changeMajor() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(4, 3);
		double[][] dA = matA.getArrayCopy2D();
		double[][] dA2 = matA.getArrayCopy2D();
		assertEqual2D(dA, dA2);
	}

	@Test
	public void inverse() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(5, 5);
		DenseMatrix matIA = matA.inverse();
		DenseMatrix matAIA = matA.times(matIA);
		DenseMatrix matI = DenseMatrix.eye(5);
		assertEqual2D(matAIA.getArrayCopy2D(), matI.getArrayCopy2D());
	}

	@Test
	public void nnls() throws Exception {
		DenseMatrix matA = DenseMatrix.rand(8, 4);
		DenseVector b = DenseVector.ones(8);
		DenseMatrix matATA = matA.transpose().times(matA);
		DenseVector vecATb = matA.transpose().times(b);
		DenseVector x = NNLSSolver.solve(matATA, vecATb);

		DenseMatrix matX = new DenseMatrix(8, 1, b.getData().clone());
		LeastSquareSolver.solve(matA.copy(), matX);
		double[] xdata = new double[4];
		System.arraycopy(matX.getArrayCopy1D(false), 0, xdata, 0, 4);
		DenseVector xLS = new DenseVector(xdata);

		System.out.println(matA.times(x).minus(b).normL2());
		System.out.println(matA.times(xLS).minus(b).normL2());
		System.out.println(matA.times(DenseVector.ones(4).scale(0.5)).minus(b).normL2());
		System.out.println(x.toString());
	}

}
