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

import java.io.Serializable;
import java.util.Arrays;

/**
 * DenseMatrix stores dense matrix data and provides some methods to operate on
 * the matrix it represents.
 */
public class DenseMatrix implements Serializable {

	/**
	 * Row dimension.
	 */
	private int m;

	/**
	 * Column dimension.
	 */
	private int n;

	/**
	 * Array for internal storage of elements.
	 *
	 * <p>The matrix data is stored in column major format internally.
	 */
	private double[] data;

	/**
	 * Construct an m-by-n matrix of zeros.
	 *
	 * @param m Number of rows.
	 * @param n Number of colums.
	 */
	public DenseMatrix(int m, int n) {
		this(m, n, new double[m * n], false);
	}

	/**
	 * Construct a matrix from a 1-D array. The data in the array should organize
	 * in column major.
	 *
	 * @param m    Number of rows.
	 * @param n    Number of cols.
	 * @param data One-dimensional array of doubles.
	 */
	public DenseMatrix(int m, int n, double[] data) {
		this(m, n, data, false);
	}

	/**
	 * Construct a matrix from a 1-D array. The data in the array is organized
	 * in column major or in row major, which is specified by parameter 'inRowMajor'
	 *
	 * @param m          Number of rows.
	 * @param n          Number of cols.
	 * @param data       One-dimensional array of doubles.
	 * @param inRowMajor Whether the matrix in 'data' is in row major format.
	 */
	public DenseMatrix(int m, int n, double[] data, boolean inRowMajor) {
		assert (data.length == m * n);
		this.m = m;
		this.n = n;
		if (inRowMajor) {
			toColumnMajor(m, n, data);
		}
		this.data = data;
	}

	/**
	 * Construct a matrix from a 2-D array.
	 *
	 * @param data Two-dimensional array of doubles.
	 * @throws IllegalArgumentException All rows must have the same size
	 */
	public DenseMatrix(double[][] data) {
		this.m = data.length;
		if (this.m == 0) {
			this.n = 0;
			this.data = new double[0];
			return;
		}
		this.n = data[0].length;
		for (int i = 0; i < m; i++) {
			if (data[i].length != n) {
				throw new IllegalArgumentException("All rows must have the same size.");
			}
		}
		this.data = new double[m * n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				this.set(i, j, data[i][j]);
			}
		}
	}

	/**
	 * Create an identity matrix.
	 *
	 * @param n
	 * @return
	 */
	public static DenseMatrix eye(int n) {
		return eye(n, n);
	}

	/**
	 * Create a identity matrix.
	 *
	 * @param m
	 * @param n
	 * @return
	 */
	public static DenseMatrix eye(int m, int n) {
		DenseMatrix mat = new DenseMatrix(m, n);
		int k = Math.min(m, n);
		for (int i = 0; i < k; i++) {
			mat.data[i * m + i] = 1.0;
		}
		return mat;
	}

	/**
	 * Create a zero matrix.
	 *
	 * @param m
	 * @param n
	 * @return
	 */
	public static DenseMatrix zeros(int m, int n) {
		return new DenseMatrix(m, n);
	}

	/**
	 * Create a matrix will all elements 1.
	 *
	 * @param m
	 * @param n
	 * @return
	 */
	public static DenseMatrix ones(int m, int n) {
		DenseMatrix mat = new DenseMatrix(m, n);
		Arrays.fill(mat.data, 1.);
		return mat;
	}

	/**
	 * Create a random matrix.
	 *
	 * @param m
	 * @param n
	 * @return
	 */
	public static DenseMatrix rand(int m, int n) {
		DenseMatrix mat = new DenseMatrix(m, n);
		for (int i = 0; i < mat.data.length; i++) {
			mat.data[i] = Math.random();
		}
		return mat;
	}

	/**
	 * Create a random symmetric matrix.
	 *
	 * @param n
	 * @return
	 */
	public static DenseMatrix randSymmetric(int n) {
		DenseMatrix mat = new DenseMatrix(n, n);
		for (int i = 0; i < n; i++) {
			for (int j = i; j < n; j++) {
				double r = Math.random();
				mat.set(i, j, r);
				if (i != j) {
					mat.set(j, i, r);
				}
			}
		}
		return mat;
	}

	/**
	 * y = func(x).
	 */
	public static void apply(DenseMatrix x, DenseMatrix y, UnaryOp func) {
		assert (x.m == y.m && x.n == y.n);
		double[] xdata = x.data;
		double[] ydata = y.data;
		assert (xdata.length == ydata.length);
		for (int i = 0; i < xdata.length; i++) {
			ydata[i] = func.f(xdata[i]);
		}
	}

	/**
	 * y = func(x1, x2).
	 */
	public static void apply(DenseMatrix x1, DenseMatrix x2, DenseMatrix y, BinaryOp func) {
		assert (x1.m == y.m && x1.n == y.n);
		assert (x2.m == y.m && x2.n == y.n);
		double[] x1data = x1.data;
		double[] x2data = x2.data;
		double[] ydata = y.data;
		assert (x1data.length == ydata.length);
		assert (x2data.length == ydata.length);
		for (int i = 0; i < ydata.length; i++) {
			ydata[i] = func.f(x1data[i], x2data[i]);
		}
	}

	/**
	 * y = func(x, alpha).
	 */
	public static void apply(DenseMatrix x, double alpha, DenseMatrix y, BinaryOp func) {
		assert (x.m == y.m && x.n == y.n);
		double[] xdata = x.data;
		double[] ydata = y.data;
		assert (xdata.length == ydata.length);
		for (int i = 0; i < xdata.length; i++) {
			ydata[i] = func.f(xdata[i], alpha);
		}
	}

	/**
	 * Compute element wise sum.
	 * \sum_ij func(x_ij)
	 */
	public static double applySum(DenseMatrix x, UnaryOp func) {
		double[] xdata = x.data;
		double s = 0.;
		for (int i = 0; i < xdata.length; i++) {
			s += func.f(xdata[i]);
		}
		return s;
	}

	/**
	 * Compute element wise sum.
	 * \sum_ij func(x1_ij, x2_ij)
	 */
	public static double applySum(DenseMatrix x1, DenseMatrix x2, BinaryOp func) {
		assert (x1.m == x2.m && x1.n == x2.n);
		double[] x1data = x1.data;
		double[] x2data = x2.data;
		double s = 0.;
		for (int i = 0; i < x1data.length; i++) {
			s += func.f(x1data[i], x2data[i]);
		}
		return s;
	}

	/**
	 * Get a single element.
	 *
	 * @param i Row index.
	 * @param j Column index.
	 * @return matA(i, j)
	 * @throws ArrayIndexOutOfBoundsException
	 */
	public double get(int i, int j) {
		return data[j * m + i];
	}

	/**
	 * Get the data array of this matrix.
	 */
	public double[] getData() {
		return this.data;
	}

	/**
	 * Get all matrix data, returned as a 2d array.
	 *
	 * @return
	 */
	public double[][] getArrayCopy2D() {
		double[][] arrayData = new double[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				arrayData[i][j] = this.get(i, j);
			}
		}
		return arrayData;
	}

	/**
	 * Get all matrix data, returned as a 1d array.
	 *
	 * @param inRowMajor Whether to return data in row major.
	 * @return
	 */
	public double[] getArrayCopy1D(boolean inRowMajor) {
		if (inRowMajor) {
			double[] arrayData = new double[m * n];
			for (int i = 0; i < m; i++) {
				for (int j = 0; j < n; j++) {
					arrayData[i * n + j] = this.get(i, j);
				}
			}
			return arrayData;
		} else {
			return this.data.clone();
		}
	}

	/**
	 * Get one row.
	 *
	 * @param row
	 * @return
	 */
	public double[] getRow(int row) {
		double[] r = new double[n];
		for (int i = 0; i < n; i++) {
			r[i] = this.get(row, i);
		}
		return r;
	}

	/**
	 * Get one column.
	 *
	 * @param col
	 * @return
	 */
	public double[] getColumn(int col) {
		double[] columnData = new double[m];
		System.arraycopy(this.data, col * m, columnData, 0, m);
		return columnData;
	}

	/**
	 * Clone the Matrix object.
	 */
	@Override
	public DenseMatrix clone() {
		return new DenseMatrix(this.m, this.n, this.data.clone(), false);
	}

	/**
	 * Create a new matrix by selecting some of the rows.
	 *
	 * @param rows
	 * @return
	 */
	public DenseMatrix selectRows(int[] rows) {
		DenseMatrix sub = new DenseMatrix(rows.length, this.n);
		for (int i = 0; i < rows.length; i++) {
			for (int j = 0; j < this.n; j++) {
				sub.set(i, j, this.get(rows[i], j));
			}
		}
		return sub;
	}

	/**
	 * Get sub matrix.
	 *
	 * @param m0
	 * @param m1
	 * @param n0
	 * @param n1
	 * @return
	 */
	public DenseMatrix getSubMatrix(int m0, int m1, int n0, int n1) {
		assert (m0 >= 0 && m1 <= m);
		assert (n0 >= 0 && n1 <= n);
		DenseMatrix sub = new DenseMatrix(m1 - m0, n1 - n0);
		for (int i = 0; i < sub.m; i++) {
			for (int j = 0; j < sub.n; j++) {
				sub.set(i, j, this.get(m0 + i, n0 + j));
			}
		}
		return sub;
	}

	/**
	 * Set all matrix elements to 'val'.
	 *
	 * @param val
	 */
	public void fillAll(double val) {
		Arrays.fill(this.data, val);
	}

	/**
	 * Set a single element.
	 *
	 * @param i Row index.
	 * @param j Column index.
	 * @param s A(i,j).
	 * @throws ArrayIndexOutOfBoundsException
	 */
	public void set(int i, int j, double s) {
		data[j * m + i] = s;
	}

	/**
	 * Add a single element.
	 *
	 * @param i Row index.
	 * @param j Column index.
	 * @param s A(i,j).
	 * @throws ArrayIndexOutOfBoundsException
	 */
	public void add(int i, int j, double s) {
		data[j * m + i] += s;
	}

	/**
	 * Set a row.
	 *
	 * @param data
	 * @param row
	 */
	public void setRowData(double[] data, int row) {
		assert data.length == n;
		for (int i = 0; i < n; i++) {
			this.set(row, i, data[i]);
		}
	}

	/**
	 * Set a column.
	 *
	 * @param data
	 * @param col
	 */
	public void setColData(double[] data, int col) {
		assert data.length == m;
		System.arraycopy(data, 0, this.data, col * m, m);
	}

	/**
	 * Set part of the matrix values from the values of another matrix.
	 */
	public void setSubMatrix(DenseMatrix sub, int m0, int m1, int n0, int n1) {
		assert (m0 >= 0 && m1 <= m);
		assert (n0 >= 0 && n1 <= n);
		for (int i = 0; i < sub.m; i++) {
			for (int j = 0; j < sub.n; j++) {
				this.set(m0 + i, n0 + j, sub.get(i, j));
			}
		}
	}

	/**
	 * Check whether the matrix is square matrix.
	 */
	public boolean isSquare() {
		return m == n;
	}

	/**
	 * Check whether the matrix is symmetric matrix.
	 */
	public boolean isSymmetric() {
		if (m != n) {
			return false;
		}
		for (int i = 0; i < n; i++) {
			for (int j = i + 1; j < n; j++) {
				if (this.get(i, j) != this.get(j, i)) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Get the number of rows.
	 */
	public int numRows() {
		return m;
	}

	/**
	 * Get the number of columns.
	 */
	public int numCols() {
		return n;
	}

	/**
	 * Element wise sum.
	 */
	public double sum() {
		return DenseMatrix.applySum(this, x -> x);
	}

	/**
	 * Element wise sum of absolute values.
	 */
	public double sumAbs() {
		return DenseMatrix.applySum(this, x -> Math.abs(x));
	}

	/**
	 * Element wise sum of square values.
	 */
	public double sumSquare() {
		return DenseMatrix.applySum(this, x -> x * x);
	}

	/**
	 * matC = matA + matB .
	 *
	 * @param matB another matrix
	 * @return matA + matB
	 */
	public DenseMatrix plus(DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(m, n);
		DenseMatrix.apply(this, matB, matC, ((a, b) -> a + b));
		return matC;
	}

	/**
	 * matB := matA + alpha .
	 */
	public DenseMatrix plus(double alpha) {
		DenseMatrix x = this.clone();
		DenseMatrix.apply(x, alpha, x, ((a, b) -> a + b));
		return x;
	}

	/**
	 * matA = matA + matB .
	 *
	 * @param matB another matrix
	 */
	public void plusEquals(DenseMatrix matB) {
		DenseMatrix.apply(this, matB, this, ((a, b) -> a + b));
	}

	/**
	 * matA := matA + alpha .
	 *
	 * @return
	 */
	public void plusEquals(double alpha) {
		DenseMatrix.apply(this, alpha, this, ((a, b) -> a + b));
	}

	/**
	 * matC = matA - matB .
	 *
	 * @param matB another matrix
	 * @return matA - matB
	 */
	public DenseMatrix minus(DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(m, n);
		DenseMatrix.apply(this, matB, matC, ((a, b) -> a - b));
		return matC;
	}

	/**
	 * matA = matA - matB .
	 *
	 * @param matB another matrix
	 */
	public void minusEquals(DenseMatrix matB) {
		DenseMatrix.apply(this, matB, this, ((a, b) -> a - b));
	}

	/**
	 * Multiply a matrix by a scalar, matC = s*matA .
	 *
	 * @param s scalar
	 * @return s*matA
	 */
	public DenseMatrix times(double s) {
		DenseMatrix matC = new DenseMatrix(m, n);
		DenseMatrix.apply(this, s, matC, ((a, b) -> a * b));
		return matC;
	}

	/**
	 * Multiply a matrix by a scalar in place, matA = s*matA .
	 *
	 * @param s scalar
	 */
	public void timesEquals(double s) {
		DenseMatrix.apply(this, s, this, ((a, b) -> a * b));
	}

	/**
	 * Linear algebraic matrix multiplication, matA * matB .
	 *
	 * @param matB another matrix
	 * @return Matrix product, matA * matB
	 * @throws IllegalArgumentException Matrix inner dimensions must agree.
	 */
	public DenseMatrix times(DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(this.m, matB.n);
		BLAS.gemm(1.0, this, false, matB, false, 0., matC);
		return matC;
	}

	/**
	 * Matrix vector multiplication: matA * x .
	 */
	public DenseVector times(DenseVector x) {
		DenseVector y = new DenseVector(this.numRows());
		BLAS.gemv(1.0, this, false, x, 0.0, y);
		return y;
	}

	/**
	 * Matrix vector multiplication: matA * x .
	 */
	public DenseVector times(SparseVector x) {
		DenseVector y = new DenseVector(this.numRows());
		for (int i = 0; i < this.numRows(); i++) {
			double s = 0.;
			int[] indices = x.getIndices();
			double[] values = x.getValues();
			for (int j = 0; j < indices.length; j++) {
				int index = indices[j];
				if (index >= this.numCols()) {
					throw new RuntimeException("Vector index out of bound:" + index);
				}
				s += this.get(i, index) * values[j];
			}
			y.set(i, s);
		}
		return y;
	}

	/**
	 * Create a new matrix by transposing current matrix.
	 * Use cache-oblivious matrix transpose algorithm.
	 *
	 * @return matA'
	 */
	public DenseMatrix transpose() {
		DenseMatrix matA = new DenseMatrix(n, m);
		int m0 = m;
		int n0 = n;
		int barrierSize = 16384;
		while (m0 * n0 > barrierSize) {
			if (m0 >= n0) {
				m0 /= 2;
			} else {
				n0 /= 2;
			}
		}
		for (int i0 = 0; i0 < m; i0 += m0) {
			for (int j0 = 0; j0 < n; j0 += n0) {
				for (int i = i0; i < i0 + m0 && i < m; i++) {
					for (int j = j0; j < j0 + n0 && j < n; j++) {
						matA.set(j, i, this.get(i, j));
					}
				}
			}
		}
		return matA;
	}

	/**
	 * Create a 1 x m matrix by summing each of the rows of a m x n matrix.
	 */
	public DenseMatrix sumByRow() {
		DenseMatrix rowSums = new DenseMatrix(1, m);
		for (int i = 0; i < this.m; i++) {
			double s = 0.;
			for (int j = 0; j < this.n; j++) {
				s += this.get(i, j);
			}
			rowSums.set(0, i, s);
		}
		return rowSums;
	}

	/**
	 * Create a 1 x n matrix by summing each of the columns of a m x n matrix.
	 */
	public DenseMatrix sumByCol() {
		DenseMatrix colSums = new DenseMatrix(1, n);
		for (int i = 0; i < this.n; i++) {
			double s = 0.;
			for (int j = 0; j < this.m; j++) {
				s += this.get(j, i);
			}
			colSums.set(0, i, s);
		}
		return colSums;
	}

	/**
	 * Converts the data layout in "data" from row major to column major.
	 */
	private static void toColumnMajor(int m, int n, double[] data) {
		if (m == n) {
			for (int i = 0; i < m; i++) {
				for (int j = i + 1; j < m; j++) {
					int pos0 = j * m + i;
					int pos1 = i * m + j;
					double t = data[pos0];
					data[pos0] = data[pos1];
					data[pos1] = t;
				}
			}
		} else {
			DenseMatrix temp = new DenseMatrix(n, m, data, false);
			System.arraycopy(temp.transpose().data, 0, data, 0, data.length);
		}
	}

	/**
	 * matC := matA .* matB .
	 */
	public static DenseMatrix elementWiseProduct(DenseMatrix matA, DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(matA.m, matA.n);
		DenseMatrix.apply(matA, matB, matC, ((a, b) -> a * b));
		return matC;
	}

	/**
	 * matC := matA ./ matB.
	 */
	public static DenseMatrix elementWiseDivide(DenseMatrix matA, DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(matA.m, matA.n);
		DenseMatrix.apply(matA, matB, matC, ((a, b) -> a / b));
		return matC;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		sbd.append(String.format("mat[%d,%d]:\n", m, n));
		for (int i = 0; i < m; i++) {
			sbd.append("  ");
			for (int j = 0; j < n; j++) {
				if (j > 0) {
					sbd.append(",");
				}
				sbd.append(this.get(i, j));
			}
			sbd.append("\n");
		}
		return sbd.toString();
	}
}
