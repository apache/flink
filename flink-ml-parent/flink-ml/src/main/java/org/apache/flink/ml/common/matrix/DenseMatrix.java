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
 * Dense Matrix.
 */
public class DenseMatrix implements Serializable {
	/* ------------------------
	Class variables
     * ------------------------ */

	/**
	 * Row and column dimensions.
	 */
	int m, n;

	/**
	 * Array for internal storage of elements.
	 * <p>
	 * The matrix data is stored in column major format internally.
	 */
	double[] data;

    /* ---------------------------------------------------
	 * Constructors
     * --------------------------------------------------- */

	/**
	 * Construct an empty matrix.
	 */
	public DenseMatrix() {
	}

	/**
	 * Construct an m-by-n matrix of zeros.
	 *
	 * @param m Number of rows.
	 * @param n Number of colums.
	 */
	public DenseMatrix(int m, int n) {
		this.m = m;
		this.n = n;
		this.data = new double[m * n];
	}

	/**
	 * Construct a matrix from a 1-D array. The data in the array should organize
	 * in row major.
	 *
	 * @param m    Number of rows.
	 * @param n    Number of cols.
	 * @param data One-dimensional array of doubles.
	 */
	public DenseMatrix(int m, int n, double[] data) {
		this(m, n, data, true);
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
			this.data = new double[m * n];
			for (int i = 0; i < m; i++) {
				for (int j = 0; j < n; j++) {
					this.set(i, j, data[i * n + j]);
				}
			}
		} else {
			this.data = data.clone();
		}
	}

	/**
	 * Construct a matrix from a 2-D array.
	 *
	 * @param matA Two-dimensional array of doubles.
	 * @throws IllegalArgumentException All rows must have the same size
	 */
	public DenseMatrix(double[][] matA) {
		this.m = matA.length;
		this.n = matA[0].length;
		for (int i = 0; i < m; i++) {
			if (matA[i].length != n) {
				throw new IllegalArgumentException("All rows must have the same size.");
			}
		}
		this.data = new double[m * n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				this.set(i, j, matA[i][j]);
			}
		}
	}

	/**
	 * Construct a matrix with provided data buffer.
	 * This is for internal use only, so it is package private.
	 *
	 * @param m    Number of rows.
	 * @param n    Number of cols.
	 * @param data One-dimensional array of doubles.
	 */
	public static DenseMatrix fromDataBuffer(int m, int n, double[] data) {
		assert m * n == data.length;
		DenseMatrix matA = new DenseMatrix();
		matA.m = m;
		matA.n = n;
		matA.data = data;
		return matA;
	}

    /* ---------------------------------------------------
	 * Handy methods for creating matrix
     * --------------------------------------------------- */

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
		DenseMatrix matM = new DenseMatrix(m, n);
		int mn = Math.min(m, n);
		for (int i = 0; i < mn; i++) {
			matM.data[i * n + i] = 1.0;
		}
		return matM;
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
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				mat.set(i, j, Math.random());
			}
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

    /* ---------------------------------------------------
     * Methods for accessing matrix data
     * --------------------------------------------------- */

	/**
	 * C := A .* B    .
	 */
	public static DenseMatrix elementWiseProduct(DenseMatrix matA, DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(matA.m, matA.n);
		DenseMatrix.apply(matA, matB, matC, ((a, b) -> a * b));
		return matC;
	}

	/**
	 * C := A ./ B   .
	 */
	public static DenseMatrix elementWiseDivide(DenseMatrix matA, DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(matA.m, matA.n);
		DenseMatrix.apply(matA, matB, matC, ((a, b) -> a / b));
		return matC;
	}

	public static DenseMatrix sumByRow(DenseMatrix dm) {
		double[][] rowSums = new double[1][dm.m];
		for (int i = 0; i < dm.m; i++) {
			rowSums[0][i] = 0;
			for (int j = 0; j < dm.n; j++) {
				rowSums[0][i] += dm.get(i, j);
			}
		}
		return new DenseMatrix(rowSums);
	}

	public static DenseMatrix sumByCol(DenseMatrix dm) {
		double[][] rowSums = new double[1][dm.n];
		for (int j = 0; j < dm.n; j++) {
			rowSums[0][j] = 0;
		}

		for (int i = 0; i < dm.m; i++) {
			for (int j = 0; j < dm.n; j++) {
				rowSums[0][j] += dm.get(i, j);
			}
		}
		return new DenseMatrix(rowSums);
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

    /* ---------------------------------------------------
     * Methods for setting matrix data
     * --------------------------------------------------- */

	/**
	 * Get a single element.
	 *
	 * @param i Row index.
	 * @param j Column index.
	 * @return A(i, j)
	 * @throws ArrayIndexOutOfBoundsException
	 */
	public double get(int i, int j) {
		return data[j * m + i];
	}

	/**
	 * Return the internal data buffer of the matrix. Be careful of the data format
	 * of the matrix.
	 */
	public double[] getDataBuffer() {
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
		double[] arrayData = new double[m * n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				if (inRowMajor) {
					arrayData[i * n + j] = this.get(i, j);
				} else {
					arrayData[j * m + i] = this.get(i, j);
				}
			}
		}
		return arrayData;
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
		double[] c = new double[m];
		for (int i = 0; i < m; i++) {
			c[i] = this.get(i, col);
		}
		return c;
	}

    /* ---------------------------------------------------
     * Methods for accessing matrix properties
     * --------------------------------------------------- */

	/**
	 * Clone the Matrix object.
	 */
	public DenseMatrix copy() {
		return DenseMatrix.fromDataBuffer(this.m, this.n, this.data.clone());
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
		for (int i = 0; i < m; i++) {
			this.set(i, col, data[i]);
		}
	}

	public DenseMatrix setSubMatrix(DenseMatrix sub, int m0, int m1, int n0, int n1) {
		assert (m0 >= 0 && m1 <= m);
		assert (n0 >= 0 && n1 <= n);
		for (int i = 0; i < sub.m; i++) {
			for (int j = 0; j < sub.n; j++) {
				this.set(m0 + i, n0 + j, sub.get(i, j));
			}
		}
		return this;
	}

	public boolean isSquared() {
		return m == n;
	}

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

    /* ---------------------------------------------------
     * Methods of matrix operations
     * --------------------------------------------------- */

	public int numRows() {
		return m;
	}

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
	 * C = A + B  .
	 *
	 * @param matB another matrix
	 * @return A + B
	 */
	public DenseMatrix plus(DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(m, n);
		DenseMatrix.apply(this, matB, matC, ((a, b) -> a + b));
		return matC;
	}

	public DenseMatrix plus(double alpha) {
		DenseMatrix x = this.copy();
		DenseMatrix.apply(x, alpha, x, ((a, b) -> a + b));
		return x;
	}

	/**
	 * A = A + B  .
	 *
	 * @param matB another matrix
	 * @return A + B
	 */
	public DenseMatrix plusEquals(DenseMatrix matB) {
		DenseMatrix.apply(this, matB, this, ((a, b) -> a + b));
		return this;
	}

	/**
	 * A := A + alpha  .
	 *
	 * @return
	 */
	public DenseMatrix plusEquals(double alpha) {
		DenseMatrix.apply(this, alpha, this, ((a, b) -> a + b));
		return this;
	}

	/**
	 * C = A - B   .
	 *
	 * @param matB another matrix
	 * @return A - B
	 */
	public DenseMatrix minus(DenseMatrix matB) {
		DenseMatrix matC = new DenseMatrix(m, n);
		DenseMatrix.apply(this, matB, matC, ((a, b) -> a - b));
		return matC;
	}

	/**
	 * A = A - B   .
	 *
	 * @param matB another matrix
	 */
	public void minusEquals(DenseMatrix matB) {
		DenseMatrix.apply(this, matB, this, ((a, b) -> a - b));
	}

	/**
	 * Multiply a matrix by a scalar, C = s*A   .
	 *
	 * @param s scalar
	 * @return s*A
	 */
	public DenseMatrix times(double s) {
		DenseMatrix matC = new DenseMatrix(m, n);
		DenseMatrix.apply(this, s, matC, ((a, b) -> a * b));
		return matC;
	}

	/**
	 * Multiply a matrix by a scalar in place, A = s*A  .
	 *
	 * @param s scalar
	 * @return replace A by s*A
	 */
	public DenseMatrix timesEquals(double s) {
		DenseMatrix.apply(this, s, this, ((a, b) -> a * b));
		return this;
	}


	/**
	 * Create a new matrix by transposing current matrix.
	 * Use cache-oblivious matrix transpose algorithm.
	 *
	 * @return A'
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

	/**
	 * Unary method.
	 */
	public interface UnaryOp {
		double f(double x);
	}

	/**
	 * Binary method.
	 */
	public interface BinaryOp {
		double f(double x, double y);
	}
}
