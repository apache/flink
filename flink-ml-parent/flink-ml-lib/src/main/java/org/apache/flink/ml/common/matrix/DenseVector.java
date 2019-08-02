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

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Random;

/**
 * A dense vector represented by a values array.
 */
public class DenseVector extends Vector {

	/**
	 * The array holding the vector data.
	 */
	private double[] data;

	/**
	 * Create a zero size vector.
	 */
	public DenseVector() {
		this(0);
	}

	/**
	 * Create a size n vector with all elements zero.
	 *
	 * @param n Size of the vector.
	 */
	public DenseVector(int n) {
		this.data = new double[n];
	}

	/**
	 * Create a dense vector with the user provided data.
	 *
	 * @param data The vector data.
	 */
	public DenseVector(double[] data) {
		this.data = data;
	}

	/**
	 * Create a dense vector with all elements one.
	 *
	 * @param n Size of the vector.
	 * @return The newly created dense vector.
	 */
	public static DenseVector ones(int n) {
		DenseVector r = new DenseVector(n);
		Arrays.fill(r.data, 1.0);
		return r;
	}

	/**
	 * Create a dense vector with all elements zero.
	 *
	 * @param n Size of the vector.
	 * @return The newly created dense vector.
	 */
	public static DenseVector zeros(int n) {
		DenseVector r = new DenseVector(n);
		Arrays.fill(r.data, 0.0);
		return r;
	}

	/**
	 * Create a dense vector with random values uniformly distributed in the range of [0.0, 1.0].
	 *
	 * @param n Size of the vector.
	 * @return The newly created dense vector.
	 */
	public static DenseVector rand(int n) {
		Random random = new Random();
		DenseVector v = new DenseVector(n);
		for (int i = 0; i < n; i++) {
			v.data[i] = random.nextDouble();
		}
		return v;
	}

	@Override
	public String serialize() {
		StringBuilder sbd = new StringBuilder();

		for (int i = 0; i < data.length; i++) {
			sbd.append(data[i]);
			if (i < data.length - 1) {
				sbd.append(",");
			}
		}
		return sbd.toString();
	}

	/**
	 * Parse the dense vector from a formatted string.
	 *
	 * @param str A string of comma separated values.
	 * @return The parsed vector.
	 */
	public static DenseVector deserialize(String str) {
		try {
			if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)) {
				return new DenseVector();
			}

			int n = StringUtils.countMatches(str, ",") + 1;
			double[] data = new double[n];
			int startPos = 0;
			for (int i = 0; i < n - 1; i++) {
				int commaPos = StringUtils.indexOf(str, ",", startPos);
				data[i] = Double.valueOf(StringUtils.substring(str, startPos, commaPos));
				startPos = commaPos + 1;
			}
			data[n - 1] = Double.valueOf(StringUtils.substring(str, startPos));
			return new DenseVector(data);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Fail to parse vector \"%s\".", str), e);
		}
	}

	/**
	 * y = func(x).
	 */
	public static void apply(DenseVector x, DenseVector y, UnaryOp func) {
		assert (x.data.length == y.data.length);
		for (int i = 0; i < x.data.length; i++) {
			y.data[i] = func.f(x.data[i]);
		}
	}

	/**
	 * y = func(x1, x2).
	 */
	public static void apply(DenseVector x1, DenseVector x2, DenseVector y, BinaryOp func) {
		assert (x1.data.length == y.data.length);
		assert (x2.data.length == y.data.length);
		for (int i = 0; i < y.data.length; i++) {
			y.data[i] = func.f(x1.data[i], x2.data[i]);
		}
	}

	/**
	 * y = func(x, alpha).
	 */
	public static void apply(DenseVector x, double alpha, DenseVector y, BinaryOp func) {
		assert (x.data.length == y.data.length);
		for (int i = 0; i < x.data.length; i++) {
			y.data[i] = func.f(x.data[i], alpha);
		}
	}

	@Override
	public DenseVector clone() {
		return new DenseVector(this.data.clone());
	}

	@Override
	public String toString() {
		return this.serialize();
	}

	@Override
	public int size() {
		return data.length;
	}

	@Override
	public double get(int i) {
		return data[i];
	}

	@Override
	public void set(int i, double d) {
		data[i] = d;
	}

	@Override
	public void add(int i, double d) {
		data[i] += d;
	}

	@Override
	public double normL1() {
		double d = 0;
		for (double t : data) {
			d += Math.abs(t);
		}
		return d;
	}

	@Override
	public double normL2() {
		double d = 0;
		for (double t : data) {
			d += t * t;
		}
		return Math.sqrt(d);
	}

	@Override
	public double normL2Square() {
		double d = 0;
		for (double t : data) {
			d += t * t;
		}
		return d;
	}

	@Override
	public double normInf() {
		double d = 0;
		for (double t : data) {
			d = Math.max(Math.abs(t), d);
		}
		return d;
	}

	@Override
	public DenseVector slice(int[] indices) {
		double[] values = new double[indices.length];
		for (int i = 0; i < indices.length; ++i) {
			if (indices[i] >= data.length) {
				throw new RuntimeException("Index is larger than vector size.");
			}
			values[i] = data[indices[i]];
		}
		return new DenseVector(values);
	}

	@Override
	public DenseVector prefix(double d) {
		double[] data = new double[this.size() + 1];
		data[0] = d;
		System.arraycopy(this.data, 0, data, 1, this.data.length);
		return new DenseVector(data);
	}

	@Override
	public DenseVector append(double d) {
		double[] data = new double[this.size() + 1];
		System.arraycopy(this.data, 0, data, 0, this.data.length);
		data[this.size()] = d;
		return new DenseVector(data);
	}

	@Override
	public void scaleEqual(double d) {
		for (int i = 0; i < this.size(); i++) {
			this.data[i] *= d;
		}
	}

	@Override
	public DenseVector plus(Vector other) {
		if (other instanceof DenseVector) {
			DenseVector r = this.clone();
			DenseVector.apply(this, (DenseVector) other, r, ((a, b) -> a + b));
			return r;
		} else {
			return (DenseVector) other.plus(this);
		}
	}

	@Override
	public DenseVector minus(Vector other) {
		if (other instanceof DenseVector) {
			DenseVector r = this.clone();
			DenseVector.apply(this, (DenseVector) other, r, ((a, b) -> a - b));
			return r;
		} else {
			return (DenseVector) other.scale(-1.0).plus(this);
		}
	}

	@Override
	public DenseVector scale(double d) {
		DenseVector r = this.clone();
		for (int i = 0; i < this.size(); i++) {
			r.data[i] *= d;
		}
		return r;
	}

	/**
	 * Set the data of the vector the same as those of another vector.
	 */
	public void setEqual(DenseVector other) {
		System.arraycopy(other.data, 0, this.data, 0, this.size());
	}

	/**
	 * this += other .
	 */
	public void plusEqual(DenseVector other) {
		DenseVector.apply(this, other, this, ((a, b) -> a + b));
	}

	/**
	 * this += alpha * other .
	 */
	public void plusScaleEqual(DenseVector other, double alpha) {
		for (int i = 0; i < this.size(); i++) {
			this.data[i] += other.data[i] * alpha;
		}
	}

	/**
	 * this -= other .
	 */
	public void minusEqual(DenseVector other) {
		DenseVector.apply(this, other, this, ((a, b) -> a - b));
	}

	/**
	 * this -= alpha * other .
	 */
	public void minusScaleEqual(DenseVector other, double alpha) {
		for (int i = 0; i < this.size(); i++) {
			this.data[i] -= other.data[i] * alpha;
		}
	}

	/**
	 * this = max(this, other) .
	 */
	public void maxEqual(DenseVector other) {
		DenseVector.apply(this, other, this, ((a, b) -> Math.max(a, b)));
	}

	/**
	 * this = min(this, other) .
	 */
	public void minEqual(DenseVector other) {
		DenseVector.apply(this, other, this, ((a, b) -> Math.min(a, b)));
	}

	@Override
	public double dot(Vector vec) {
		if (vec instanceof DenseVector) {
			DenseVector other = (DenseVector) vec;
			double d = 0;
			for (int i = 0; i < this.size(); i++) {
				d += this.data[i] * other.data[i];
			}
			return d;
		} else {
			return ((SparseVector) vec).dot(this);
		}
	}

	/**
	 * Round each elements and create a new vector to store the result.
	 *
	 * @return The newly created dense vector.
	 */
	public DenseVector round() {
		DenseVector r = new DenseVector(this.size());
		for (int i = 0; i < this.size(); i++) {
			r.data[i] = Math.round(this.data[i]);
		}
		return r;
	}

	/**
	 * Round each elements inplace.
	 */
	public void roundEqual() {
		for (int i = 0; i < this.size(); i++) {
			this.data[i] = Math.round(this.data[i]);
		}
	}

	/**
	 * Compute the outer product with itself.
	 *
	 * @return The outer product matrix.
	 */
	public DenseMatrix outer() {
		return this.outer(this);
	}

	/**
	 * Compute the outer product with another vector.
	 *
	 * @return The outer product matrix.
	 */
	public DenseMatrix outer(DenseVector other) {
		int nrows = this.size();
		int ncols = other.size();
		double[] data = new double[nrows * ncols];
		int pos = 0;
		for (int j = 0; j < ncols; j++) {
			for (int i = 0; i < nrows; i++) {
				data[pos++] = this.data[i] * other.data[j];
			}
		}
		return new DenseMatrix(nrows, ncols, data, false);
	}

	/**
	 * Get the data array.
	 */
	public double[] getData() {
		return this.data;
	}

	/**
	 * Set the data array.
	 */
	public void setData(double[] data) {
		this.data = data;
	}

	@Override
	public void standardizeEqual(double mean, double stdvar) {
		int size = data.length;
		for (int i = 0; i < size; i++) {
			data[i] -= mean;
			data[i] *= (1.0 / stdvar);
		}
	}

	@Override
	public void normalizeEqual(double p) {
		double norm = 0.0;
		if (Double.isInfinite(p)) {
			for (int i = 0; i < data.length; i++) {
				norm = Math.max(norm, Math.abs(data[i]));
			}
		} else if (p == 1.0) {
			for (int i = 0; i < data.length; i++) {
				norm += Math.abs(data[i]);
			}
		} else if (p == 2.0) {
			for (int i = 0; i < data.length; i++) {
				norm += data[i] * data[i];
			}
			norm = Math.sqrt(norm);
		} else {
			for (int i = 0; i < data.length; i++) {
				norm += Math.pow(Math.abs(data[i]), p);
			}
			norm = Math.pow(norm, 1 / p);
		}
		for (int i = 0; i < data.length; i++) {
			data[i] /= norm;
		}
	}

	@Override
	public VectorIterator iterator() {
		return new DenseVectorIterator();
	}

	private class DenseVectorIterator implements VectorIterator {
		private int cursor = 0;

		@Override
		public boolean hasNext() {
			return cursor < data.length;
		}

		@Override
		public void next() {
			cursor++;
		}

		@Override
		public int getIndex() {
			if (cursor >= data.length) {
				throw new RuntimeException("Iterator out of bound.");
			}
			return cursor;
		}

		@Override
		public double getValue() {
			if (cursor >= data.length) {
				throw new RuntimeException("Iterator out of bound.");
			}
			return data[cursor];
		}
	}
}
