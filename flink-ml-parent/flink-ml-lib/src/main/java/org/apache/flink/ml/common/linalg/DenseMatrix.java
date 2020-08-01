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

import java.io.Serializable;
import java.util.Arrays;

/**
 * DenseMatrix stores dense matrix data and provides some methods to operate on
 * the matrix it represents.
 */
public class DenseMatrix implements Serializable {

	/**
	 * Row dimension.
	 *
	 * <p>Package private to allow access from {@link MatVecOp} and {@link BLAS}.
	 */
	int m;

	/**
	 * Column dimension.
	 *
	 * <p>Package private to allow access from {@link MatVecOp} and {@link BLAS}.
	 */
	int n;

	/**
	 * Array for internal storage of elements.
	 *
	 * <p>Package private to allow access from {@link MatVecOp} and {@link BLAS}.
	 *
	 * <p>The matrix data is stored in column major format internally.
	 */
	double[] data;

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
	 * @param n the dimension of the eye matrix.
	 * @return an identity matrix.
	 */
	public static DenseMatrix eye(int n) {
		return eye(n, n);
	}

	/**
	 * Create a m * n identity matrix.
	 *
	 * @param m the row dimension.
	 * @param n the column dimension.e
	 * @return the m * n identity matrix.
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
	 * @param m the row dimension.
	 * @param n the column dimension.
	 * @return a m * n zero matrix.
	 */
	public static DenseMatrix zeros(int m, int n) {
		return new DenseMatrix(m, n);
	}

	/**
	 * Create a matrix with all elements set to 1.
	 *
	 * @param m the row dimension
	 * @param n the column dimension
	 * @return the m * n matrix with all elements set to 1.
	 */
	public static DenseMatrix ones(int m, int n) {
		DenseMatrix mat = new DenseMatrix(m, n);
		Arrays.fill(mat.data, 1.);
		return mat;
	}

	/**
	 * Create a random matrix.
	 *
	 * @param m the row dimension
	 * @param n the column dimension.
	 * @return a m * n random matrix.
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
	 * @param n the dimension of the symmetric matrix.
	 * @return a n * n random symmetric matrix.
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
	 *
	 * @return the data array of this matrix.
	 */
	public double[] getData() {
		return this.data;
	}

	/**
	 * Get all the matrix data, returned as a 2-D array.
	 *
	 * @return all matrix data, returned as a 2-D array.
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
	 * Get all matrix data, returned as a 1-D array.
	 *
	 * @param inRowMajor Whether to return data in row major.
	 * @return all matrix data, returned as a 1-D array.
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
	 * @param row the row index.
	 * @return the row with the given index.
	 */
	public double[] getRow(int row) {
		assert (row >= 0 && row < m) : "Invalid row index.";
		double[] r = new double[n];
		for (int i = 0; i < n; i++) {
			r[i] = this.get(row, i);
		}
		return r;
	}

	/**
	 * Get one column.
	 *
	 * @param col the column index.
	 * @return the column with the given index.
	 */
	public double[] getColumn(int col) {
		assert (col >= 0 && col < n) : "Invalid column index.";
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
	 * @param rows the array of row indexes to select.
	 * @return a new matrix by selecting some of the rows.
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
	 * @param m0 the starting row index (inclusive)
	 * @param m1 the ending row index (exclusive)
	 * @param n0 the starting column index (inclusive)
	 * @param n1 the ending column index (exclusive)
	 * @return the specified sub matrix.
	 */
	public DenseMatrix getSubMatrix(int m0, int m1, int n0, int n1) {
		assert (m0 >= 0 && m1 <= m) && (n0 >= 0 && n1 <= n) : "Invalid index range.";
		DenseMatrix sub = new DenseMatrix(m1 - m0, n1 - n0);
		for (int i = 0; i < sub.m; i++) {
			for (int j = 0; j < sub.n; j++) {
				sub.set(i, j, this.get(m0 + i, n0 + j));
			}
		}
		return sub;
	}

	/**
	 * Set part of the matrix values from the values of another matrix.
	 *
	 * @param sub the matrix whose element values will be assigned to the sub matrix of this matrix.
	 * @param m0  the starting row index (inclusive)
	 * @param m1  the ending row index (exclusive)
	 * @param n0  the starting column index (inclusive)
	 * @param n1  the ending column index (exclusive)
	 */
	public void setSubMatrix(DenseMatrix sub, int m0, int m1, int n0, int n1) {
		assert (m0 >= 0 && m1 <= m) && (n0 >= 0 && n1 <= n) : "Invalid index range.";
		for (int i = 0; i < sub.m; i++) {
			for (int j = 0; j < sub.n; j++) {
				this.set(m0 + i, n0 + j, sub.get(i, j));
			}
		}
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
	 * Add the given value to a single element.
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
	 * Check whether the matrix is square matrix.
	 *
	 * @return <code>true</code> if this matrix is a square matrix, <code>false</code> otherwise.
	 */
	public boolean isSquare() {
		return m == n;
	}

	/**
	 * Check whether the matrix is symmetric matrix.
	 *
	 * @return <code>true</code> if this matrix is a symmetric matrix, <code>false</code> otherwise.
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
	 *
	 * @return the number of rows.
	 */
	public int numRows() {
		return m;
	}

	/**
	 * Get the number of columns.
	 *
	 * @return the number of columns.
	 */
	public int numCols() {
		return n;
	}

	/**
	 * Sum of all elements of the matrix.
	 */
	public double sum() {
		double s = 0.;
		for (int i = 0; i < this.data.length; i++) {
			s += this.data[i];
		}
		return s;
	}

	/**
	 * Scale the vector by value "v" and create a new matrix to store the result.
	 */
	public DenseMatrix scale(double v) {
		DenseMatrix r = this.clone();
		BLAS.scal(v, r);
		return r;
	}

	/**
	 * Scale the matrix by value "v".
	 */
	public void scaleEqual(double v) {
		BLAS.scal(v, this);
	}

	/**
	 * Create a new matrix by plussing another matrix.
	 */
	public DenseMatrix plus(DenseMatrix mat) {
		DenseMatrix r = this.clone();
		BLAS.axpy(1.0, mat, r);
		return r;
	}

	/**
	 * Create a new matrix by plussing a constant.
	 */
	public DenseMatrix plus(double alpha) {
		DenseMatrix r = this.clone();
		for (int i = 0; i < r.data.length; i++) {
			r.data[i] += alpha;
		}
		return r;
	}

	/**
	 * Plus with another matrix.
	 */
	public void plusEquals(DenseMatrix mat) {
		BLAS.axpy(1.0, mat, this);
	}

	/**
	 * Plus with a constant.
	 */
	public void plusEquals(double alpha) {
		for (int i = 0; i < this.data.length; i++) {
			this.data[i] += alpha;
		}
	}

	/**
	 * Create a new matrix by subtracting another matrix.
	 */
	public DenseMatrix minus(DenseMatrix mat) {
		DenseMatrix r = this.clone();
		BLAS.axpy(-1.0, mat, r);
		return r;
	}

	/**
	 * Minus with another vector.
	 */
	public void minusEquals(DenseMatrix mat) {
		BLAS.axpy(-1.0, mat, this);
	}

	/**
	 * Multiply with another matrix.
	 */
	public DenseMatrix multiplies(DenseMatrix mat) {
		DenseMatrix r = new DenseMatrix(this.m, mat.n);
		BLAS.gemm(1.0, this, false, mat, false, 0., r);
		return r;
	}

	/**
	 * Multiply with a dense vector.
	 */
	public DenseVector multiplies(DenseVector x) {
		DenseVector y = new DenseVector(this.numRows());
		BLAS.gemv(1.0, this, false, x, 0.0, y);
		return y;
	}

	/**
	 * Multiply with a sparse vector.
	 */
	public DenseVector multiplies(SparseVector x) {
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
	 *
	 * <p>Use cache-oblivious matrix transpose algorithm.
	 */
	public DenseMatrix transpose() {
		DenseMatrix mat = new DenseMatrix(n, m);
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
						mat.set(j, i, this.get(i, j));
					}
				}
			}
		}
		return mat;
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
