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

/**
 * A utility class that wraps netlib BLAS and provides some operations on dense matrix
 * and dense vector.
 */
public class BLAS {
	private static final com.github.fommil.netlib.BLAS BLAS = com.github.fommil.netlib.BLAS.getInstance();

	/**
	 * y += a * x .
	 */
	public static void axpy(double a, double[] x, double[] y) {
		BLAS.daxpy(x.length, a, x, 1, y, 1);
	}

	public static void axpy(double a, DenseVector x, DenseVector y) {
		axpy(a, x.getData(), y.getData());
	}

	/**
	 * x \cdot y .
	 */
	public static double dot(double[] x, double[] y) {
		return BLAS.ddot(x.length, x, 1, y, 1);
	}

	public static double dot(DenseVector x, DenseVector y) {
		return dot(x.getData(), y.getData());
	}

	/**
	 * x = x * a .
	 */
	public static void scal(double a, double[] x) {
		BLAS.dscal(x.length, a, x, 1);
	}

	public static void scal(double a, DenseVector x) {
		scal(a, x.getData());
	}

	/**
	 * || x - y ||^2 .
	 */
	public static double dsquared(double[] x, double[] y) {
		double s = 0.;
		for (int i = 0; i < x.length; i++) {
			double d = x[i] - y[i];
			s += d * d;
		}
		return s;
	}

	/**
	 * | x - y | .
	 */
	public static double dabs(double[] x, double[] y) {
		double s = 0.;
		for (int i = 0; i < x.length; i++) {
			double d = x[i] - y[i];
			s += Math.abs(d);
		}
		return s;
	}

	/**
	 * C := alpha * A * B + beta * C .
	 */
	public static void gemm(double alpha, DenseMatrix matA, boolean transA, DenseMatrix matB, boolean transB,
							double beta, DenseMatrix matC) {
		if (transA) {
			assert matA.numCols() == matC.numRows();
		} else {
			assert matA.numRows() == matC.numRows();
		}
		if (transB) {
			assert matB.numRows() == matC.numCols();
		} else {
			assert matB.numCols() == matC.numCols();
		}

		final int m = matC.numRows();
		final int n = matC.numCols();
		final int k = transA ? matA.numRows() : matA.numCols();
		final int lda = matA.numRows();
		final int ldb = matB.numRows();
		final int ldc = matC.numRows();
		final String ta = transA ? "T" : "N";
		final String tb = transB ? "T" : "N";
		BLAS.dgemm(ta, tb, m, n, k, alpha, matA.data, lda, matB.data, ldb, beta, matC.data, ldc);
	}


	/**
	 * y := alpha * A * x + beta * y .
	 */
	public static void gemv(double alpha, DenseMatrix matA, boolean transA,
							DenseVector x, double beta, DenseVector y) {
		if (transA) {
			assert (matA.numCols() == y.size());
			assert (matA.numRows() == x.size());
		} else {
			assert (matA.numRows() == y.size());
			assert (matA.numCols() == x.size());
		}
		final int m = matA.numRows();
		final int n = matA.numCols();
		final int lda = matA.numRows();
		final String ta = transA ? "T" : "N";
		BLAS.dgemv(ta, m, n, alpha, matA.data, lda, x.getData(), 1, beta, y.getData(), 1);
	}
}
