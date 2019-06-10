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

import java.util.Random;

/**
 * A dense vector represented by a values array.
 */
public class DenseVector extends Vector {

	/**
	 * Vector data.
	 */
	double[] data;

	public DenseVector() {
		this(0);
	}

	public DenseVector(int n) {
		this.data = new double[n];
	}

	public DenseVector(double[] data) {
		this.data = data.clone();
	}

	@Override
	public DenseVector clone() {
		DenseVector c = new DenseVector();
		c.setData(this.data.clone());
		return c;
	}

	public static DenseVector ones(int n) {
		DenseVector r = new DenseVector(n);
		for (int i = 0; i < r.data.length; i++) {
			r.data[i] = 1.0;
		}
		return r;
	}

	public static DenseVector zeros(int n) {
		DenseVector r = new DenseVector(n);
		for (int i = 0; i < r.data.length; i++) {
			r.data[i] = 0.0;
		}
		return r;
	}

	public static DenseVector rand(int n) {
		Random random = new Random();
		DenseVector v = new DenseVector(n);
		for (int i = 0; i < n; i++) {
			v.set(i, random.nextDouble());
		}
		return v;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();

		for (int i = 0; i < data.length; i++) {
			sbd.append(data[i]);
			if (i < data.length - 1) {
				sbd.append(",");
			}
		}

		return sbd.toString();
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
		double[] newVec = new double[this.size() + 1];
		newVec[0] = d;
		for (int i = 0; i < this.size(); i++) {
			newVec[i + 1] = this.get(i);
		}
		return new DenseVector(newVec);
	}

	@Override
	public DenseVector append(double d) {
		double[] newVec = new double[this.size() + 1];
		for (int i = 0; i < this.size(); i++) {
			newVec[i] = this.get(i);
		}
		newVec[this.size()] = d;
		return new DenseVector(newVec);
	}

	@Override
	public void scaleEqual(double d) {
		for (int i = 0; i < this.size(); i++) {
			this.data[i] *= d;
		}
	}

	public DenseVector plus(DenseVector other) {
		DenseVector r = this.clone();
		DenseVector.apply(this, other, r, ((a, b) -> a + b));
		return r;
	}

	@Override
	public Vector plus(Vector other) {
		if (other instanceof DenseVector) {
			return plus((DenseVector) other);
		} else {
			return ((SparseVector) other).plus(this);
		}
	}

	public DenseVector minus(DenseVector other) {
		DenseVector r = this.clone();
		DenseVector.apply(this, other, r, ((a, b) -> a - b));
		return r;
	}

	@Override
	public Vector minus(Vector other) {
		if (other instanceof DenseVector) {
			return minus((DenseVector) other);
		} else {
			return ((SparseVector) other).scale(-1.0).plus(this);
		}
	}

	@Override
	public DenseVector scale(double d) {
		DenseVector r = new DenseVector(this.data);
		for (int i = 0; i < this.size(); i++) {
			r.data[i] *= d;
		}
		return r;
	}

	public void setEqual(DenseVector other) {
		System.arraycopy(other.data, 0, this.data, 0, this.size());
	}

	public void plusEqual(DenseVector other) {
		DenseVector.apply(this, other, this, ((a, b) -> a + b));
	}

	public void plusEqual(DenseVector v1, DenseVector v2) {
		DenseVector.apply(v1, v2, this, ((a, b) -> a + b));
	}

	public void plusEqual(double d) {
		DenseVector.apply(this, d, this, ((a, b) -> a + b));
	}

	public void plusScaleEqual(Vector vec, double val) {
		if (vec instanceof SparseVector) {
			SparseVector spvec = (SparseVector) vec;
			int[] indices = spvec.getIndices();
			double[] values = spvec.getValues();
			int size = indices.length;
			for (int i = 0; i < size; ++i) {
				this.add(indices[i], values[i] * val);
			}
		} else {
			double[] vecdata = ((DenseVector) vec).data;
			for (int i = 0; i < this.size(); i++) {
				this.data[i] += vecdata[i] * val;
			}
		}
	}

	public void minusEqual(DenseVector v1, DenseVector v2) {
		DenseVector.apply(v1, v2, this, ((a, b) -> a - b));
	}

	public void minusEqual(DenseVector other) {
		DenseVector.apply(this, other, this, ((a, b) -> a - b));
	}

	public void minusEqual(double d) {
		DenseVector.apply(this, d, this, ((a, b) -> a - b));
	}

	public void minusScaleEqual(DenseVector other, Double alpha) {
		for (int i = 0; i < this.size(); i++) {
			this.data[i] = this.data[i] - other.data[i] * alpha;
		}
	}

	public void timeEqual(DenseVector other) {
		DenseVector.apply(this, other, this, ((a, b) -> a * b));
	}

	public void maxEqual(DenseVector other) {
		DenseVector.apply(this, other, this, ((a, b) -> Math.max(a, b)));
	}

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

	public DenseVector round() {
		DenseVector r = new DenseVector(this.size());
		for (int i = 0; i < this.size(); i++) {
			r.data[i] = Math.round(this.data[i]);
		}
		return r;
	}

	public void roundEqual() {
		for (int i = 0; i < this.size(); i++) {
			this.data[i] = Math.round(this.data[i]);
		}
	}

	public DenseMatrix outer() {
		return this.outer(this);
	}

	public DenseMatrix outer(DenseVector other) {
		int nrows = this.size();
		int ncols = other.size();
		double[][] matA = new double[nrows][ncols];
		for (int i = 0; i < nrows; i++) {
			for (int j = 0; j < ncols; j++) {
				matA[i][j] = this.data[i] * other.data[j];
			}
		}
		return new DenseMatrix(matA);
	}

	public double[] getArrayCopy() {
		return this.data.clone();
	}

	public double[] getData() {
		return this.data;
	}

	public void setData(double[] data) {
		this.data = data;
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

	public static DenseVector deserialize(String str) {
		try {
			str = StringUtils.trim(str);

			if (str.isEmpty()) {
				return new DenseVector();
			}

			int numValues = StringUtils.countMatches(str, ",") + 1;
			double[] data = new double[numValues];

			int startPos = 0;
			int endPos;
			for (int i = 0; i < numValues; i++) {
				// extract the value string
				endPos = StringUtils.indexOf(str, ",", startPos);
				if (endPos == -1) {
					endPos = str.length();
				}
				String valueStr = StringUtils.substring(str, startPos, endPos);
				startPos = endPos + 1;
				data[i] = Double.valueOf(valueStr);
			}
			DenseVector vector = new DenseVector();
			vector.setData(data);
			return vector;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Fail to parse vector \"" + str + "\".");
		}
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

	public DenseVector power(double p) {
		double[] outData = new double[data.length];
		for (int i = 0; i < outData.length; i++) {
			outData[i] = Math.pow(data[i], p);
		}
		return new DenseVector(outData);
	}

	@Override
	public VectorIterator iterator() {
		return new DenseVectorIterator();
	}

	/**
	 * y = func(x).
	 */
	public static void apply(DenseVector x, DenseVector y, UnaryOp func) {
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
	public static void apply(DenseVector x1, DenseVector x2, DenseVector y, BinaryOp func) {
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
	public static void apply(DenseVector x, double alpha, DenseVector y, BinaryOp func) {
		double[] xdata = x.data;
		double[] ydata = y.data;
		assert (xdata.length == ydata.length);
		for (int i = 0; i < xdata.length; i++) {
			ydata[i] = func.f(xdata[i], alpha);
		}
	}
}
