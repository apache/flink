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

/**
 * A utility class that provides BLAS routines over matrices and vectors.
 */
public class BLAS {
	private static final com.github.fommil.netlib.BLAS NATIVE_BLAS = com.github.fommil.netlib.BLAS.getInstance();
	private static final com.github.fommil.netlib.BLAS F2J_BLAS = com.github.fommil.netlib.F2jBLAS.getInstance();

	/**
	 * y += a * x .
	 */
	public static void axpy(double a, double[] x, double[] y) {
		assert x.length == y.length : "Array dimension mismatched.";
		F2J_BLAS.daxpy(x.length, a, x, 1, y, 1);
	}

	/**
	 * y += a * x .
	 */
	public static void axpy(double a, DenseVector x, DenseVector y) {
		assert x.data.length == y.data.length : "Vector dimension mismatched.";
		F2J_BLAS.daxpy(x.data.length, a, x.data, 1, y.data, 1);
	}

	/**
	 * y += a * x .
	 */
	public static void axpy(double a, SparseVector x, DenseVector y) {
		for (int i = 0; i < x.indices.length; i++) {
			y.data[x.indices[i]] += a * x.values[i];
		}
	}

	/**
	 * y += a * x .
	 */
	public static void axpy(double a, DenseMatrix x, DenseMatrix y) {
		assert x.m == y.m && x.n == y.n : "Matrix dimension mismatched.";
		F2J_BLAS.daxpy(x.data.length, a, x.data, 1, y.data, 1);
	}

	/**
	 * x \cdot y .
	 */
	public static double dot(double[] x, double[] y) {
		assert x.length == y.length : "Array dimension mismatched.";
		return F2J_BLAS.ddot(x.length, x, 1, y, 1);
	}

	/**
	 * x \cdot y .
	 */
	public static double dot(DenseVector x, DenseVector y) {
		assert x.data.length == y.data.length : "Vector dimension mismatched.";
		return F2J_BLAS.ddot(x.data.length, x.data, 1, y.data, 1);
	}

	/**
	 * x = x * a .
	 */
	public static void scal(double a, double[] x) {
		F2J_BLAS.dscal(x.length, a, x, 1);
	}

	/**
	 * x = x * a .
	 */
	public static void scal(double a, DenseVector x) {
		F2J_BLAS.dscal(x.data.length, a, x.data, 1);
	}

	/**
	 * x = x * a .
	 */
	public static void scal(double a, SparseVector x) {
		F2J_BLAS.dscal(x.values.length, a, x.values, 1);
	}

	/**
	 * x = x * a .
	 */
	public static void scal(double a, DenseMatrix x) {
		F2J_BLAS.dscal(x.data.length, a, x.data, 1);
	}

	/**
	 * C := alpha * A * B + beta * C .
	 */
	public static void gemm(double alpha, DenseMatrix matA, boolean transA, DenseMatrix matB, boolean transB,
							double beta, DenseMatrix matC) {
		if (transA) {
			assert matA.numCols() == matC.numRows() : "The columns of A does not match the rows of C";
		} else {
			assert matA.numRows() == matC.numRows() : "The rows of A does not match the rows of C";
		}
		if (transB) {
			assert matB.numRows() == matC.numCols() : "The rows of B does not match the columns of C";
		} else {
			assert matB.numCols() == matC.numCols() : "The columns of B does not match the columns of C";
		}

		final int m = matC.numRows();
		final int n = matC.numCols();
		final int k = transA ? matA.numRows() : matA.numCols();
		final int lda = matA.numRows();
		final int ldb = matB.numRows();
		final int ldc = matC.numRows();
		final String ta = transA ? "T" : "N";
		final String tb = transB ? "T" : "N";
		NATIVE_BLAS.dgemm(ta, tb, m, n, k, alpha, matA.getData(), lda, matB.getData(), ldb, beta, matC.getData(), ldc);
	}

	/**
	 * y := alpha * A * x + beta * y .
	 */
	public static void gemv(double alpha, DenseMatrix matA, boolean transA,
							DenseVector x, double beta, DenseVector y) {
		if (transA) {
			assert (matA.numCols() == y.size() && matA.numRows() == x.size()) : "Matrix and vector size mismatched.";
		} else {
			assert (matA.numRows() == y.size() && matA.numCols() == x.size()) : "Matrix and vector size mismatched.";
		}
		final int m = matA.numRows();
		final int n = matA.numCols();
		final int lda = matA.numRows();
		final String ta = transA ? "T" : "N";
		NATIVE_BLAS.dgemv(ta, m, n, alpha, matA.getData(), lda, x.getData(), 1, beta, y.getData(), 1);
	}
}
