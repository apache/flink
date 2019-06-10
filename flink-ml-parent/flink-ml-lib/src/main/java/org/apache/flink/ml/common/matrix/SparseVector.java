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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A sparse vector represented by an indices array and a values array.
 */
public class SparseVector extends Vector {

	/**
	 * Size of the vector. n = -1 indicates that the vector size is undetermined.
	 */
	int n;

	/**
	 * Column indices.
	 */
	int[] indices;

	/**
	 * Column values.
	 */
	double[] values;

	/**
	 * Construct an empty sparse vector with undetermined size.
	 */
	public SparseVector() {
		this(-1);
	}

	/**
	 * Construct an empty sparse vector with determined size.
	 */
	public SparseVector(int n) {
		this.n = n;
		this.indices = new int[0];
		this.values = new double[0];
	}

	/**
	 * Construct a sparse vector with the given indices and values.
	 *
	 * @throws IllegalArgumentException If size of indices array and values array differ.
	 * @throws IllegalArgumentException If n >= 0 and the indices are out of bound.
	 */
	public SparseVector(int n, int[] indices, double[] values) {
		this.n = n;
		this.indices = indices.clone();
		this.values = values.clone();
		checkIndices();
		sortIndices();
	}

	/**
	 * Construct a sparse vector with given indices to values map.
	 *
	 * @throws IllegalArgumentException If n >= 0 and the indices are out of bound.
	 */
	public SparseVector(int n, Map<Integer, Double> kv) {
		this.n = n;
		int nnz = kv.size();
		int[] indices = new int[nnz];
		double[] values = new double[nnz];

		int pos = 0;
		for (Map.Entry<Integer, Double> entry : kv.entrySet()) {
			indices[pos] = entry.getKey();
			values[pos] = entry.getValue();
			pos++;
		}

		this.indices = indices;
		this.values = values;
		checkIndices();

		if (!(kv instanceof TreeMap)) {
			sortIndices();
		}
	}

	private void checkIndices() {
		if (indices.length != values.length) {
			throw new IllegalArgumentException("Indices size and values size should be the same.");
		}
		for (int i = 0; i < indices.length; i++) {
			if (indices[i] < 0 || (n >= 0 && indices[i] >= n)) {
				throw new IllegalArgumentException("Index out of bound.");
			}
		}
	}

	private void sortIndices() {
		boolean outOfOrder = false;
		for (int i = 0; i < this.indices.length - 1; i++) {
			if (this.indices[i] >= this.indices[i + 1]) {
				outOfOrder = true;
				break;
			}
		}

		if (!outOfOrder) {
			return;
		}

		// sort
		Integer[] order = new Integer[this.indices.length];
		for (int i = 0; i < order.length; i++) {
			order[i] = i;
		}

		Arrays.sort(order, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				if (indices[o1] < indices[o2]) {
					return -1;
				} else if (indices[o1] > indices[o2]) {
					return 1;
				} else {
					return 0;
				}
			}
		});

		int nnz = this.indices.length;
		int[] sortedIndices = new int[nnz];
		double[] sortedValues = new double[nnz];

		for (int i = 0; i < order.length; i++) {
			sortedValues[i] = this.values[order[i]];
			sortedIndices[i] = this.indices[order[i]];
		}

		this.indices = sortedIndices;
		this.values = sortedValues;
	}

	@Override
	public SparseVector clone() {
		SparseVector vec = new SparseVector(this.n);
		vec.indices = this.indices.clone();
		vec.values = this.values.clone();
		return vec;
	}

	@Override
	public SparseVector prefix(double d) {
		int[] indices = new int[this.indices.length + 1];
		double[] values = new double[this.values.length + 1];
		int n = (this.n >= 0) ? this.n + 1 : this.n;

		indices[0] = 0;
		values[0] = d;

		for (int i = 0; i < this.indices.length; i++) {
			indices[i + 1] = this.indices[i] + 1;
			values[i + 1] = this.values[i];
		}

		return new SparseVector(n, indices, values);
	}

	@Override
	public SparseVector append(double d) {
		int[] indices = new int[this.indices.length + 1];
		double[] values = new double[this.values.length + 1];
		int n = (this.n >= 0) ? this.n + 1 : this.n;

		int i;
		for (i = 0; i < this.indices.length; i++) {
			indices[i] = this.indices[i];
			values[i] = this.values[i];
		}

		indices[i] = n - 1;
		values[i] = d;

		return new SparseVector(n, indices, values);
	}

	public int[] getIndices() {
		return indices;
	}

	public double[] getValues() {
		return values;
	}

	public int getMaxIndex() {
		if (indices.length <= 0) {
			return -1;
		}
		return indices[indices.length - 1];
	}

	@Override
	public int size() {
		return n;
	}

	@Override
	public double get(int i) {
		int pos = Arrays.binarySearch(indices, i);
		if (pos >= 0) {
			return values[pos];
		}
		return 0.;
	}

	public void setSize(int n) {
		this.n = n;
	}

	public int numberOfValues() {
		return this.values.length;
	}

	@Override
	public void set(int i, double val) {
		int pos = Arrays.binarySearch(indices, i);
		if (pos >= 0) {
			this.values[pos] = val;
		} else {
			pos = -(pos + 1);
			double[] newValues = new double[this.values.length + 1];
			int[] newIndices = new int[this.values.length + 1];
			int j = 0;
			for (; j < pos; j++) {
				newValues[j] = this.values[j];
				newIndices[j] = this.indices[j];
			}
			newValues[j] = val;
			newIndices[j] = i;
			for (; j < this.values.length; j++) {
				newValues[j + 1] = this.values[j];
				newIndices[j + 1] = this.indices[j];
			}
			this.values = newValues;
			this.indices = newIndices;
		}
	}

	@Override
	public void add(int i, double val) {
		int pos = Arrays.binarySearch(indices, i);
		if (pos >= 0) {
			this.values[pos] += val;
		} else {
			pos = -(pos + 1);
			double[] newValues = new double[this.values.length + 1];
			int[] newIndices = new int[this.values.length + 1];
			int j = 0;
			for (; j < pos; j++) {
				newValues[j] = this.values[j];
				newIndices[j] = this.indices[j];
			}
			newValues[j] = val;
			newIndices[j] = i;
			for (; j < this.values.length; j++) {
				newValues[j + 1] = this.values[j];
				newIndices[j + 1] = this.indices[j];
			}
			this.values = newValues;
			this.indices = newIndices;
		}
	}

	@Override
	public String toString() {
		return "Sparse Vector{" +
			"indices=" + Arrays.toString(indices) +
			"values=" + Arrays.toString(values) +
			"vectorSize=" + n +
			'}';
	}

	@Override
	public double normL2() {
		double d = 0;
		for (double t : values) {
			d += t * t;
		}
		return Math.sqrt(d);
	}

	@Override
	public double normL1() {
		double d = 0;
		for (double t : values) {
			d += Math.abs(t);
		}
		return d;
	}

	@Override
	public double normInf() {
		double d = 0;
		for (double t : values) {
			d = Math.max(Math.abs(t), d);
		}
		return d;
	}

	@Override
	public double normL2Square() {
		double d = 0;
		for (double t : values) {
			d += t * t;
		}
		return d;
	}

	@Override
	public SparseVector slice(int[] indices) {
		SparseVector sliced = new SparseVector(indices.length);
		int nnz = 0;
		sliced.indices = new int[indices.length];
		sliced.values = new double[indices.length];

		for (int i = 0; i < indices.length; i++) {
			int pos = Arrays.binarySearch(this.indices, indices[i]);
			if (pos >= 0) {
				sliced.indices[nnz] = i;
				sliced.values[nnz] = this.values[pos];
				nnz++;
			}
		}

		if (nnz < sliced.indices.length) {
			sliced.indices = Arrays.copyOf(sliced.indices, nnz);
			sliced.values = Arrays.copyOf(sliced.values, nnz);
		}

		return sliced;
	}

	public SparseVector plus(SparseVector other) {
		if (this.size() != other.size()) {
			throw new RuntimeException("The size of the two vectors are different.");
		}

		int totNnz = this.values.length + other.values.length;
		int p0 = 0;
		int p1 = 0;
		while (p0 < this.values.length && p1 < other.values.length) {
			if (this.indices[p0] == other.indices[p1]) {
				totNnz--;
				p0++;
				p1++;
			} else if (this.indices[p0] < other.indices[p1]) {
				p0++;
			} else {
				p1++;
			}
		}

		SparseVector r = new SparseVector(this.size());
		r.indices = new int[totNnz];
		r.values = new double[totNnz];
		p0 = p1 = 0;
		int pos = 0;
		while (pos < totNnz) {
			if (p0 < this.values.length && p1 < other.values.length) {
				if (this.indices[p0] == other.indices[p1]) {
					r.indices[pos] = this.indices[p0];
					r.values[pos] = this.values[p0] + other.values[p1];
					p0++;
					p1++;
				} else if (this.indices[p0] < other.indices[p1]) {
					r.indices[pos] = this.indices[p0];
					r.values[pos] = this.values[p0];
					p0++;
				} else {
					r.indices[pos] = other.indices[p1];
					r.values[pos] = other.values[p1];
					p1++;
				}
				pos++;
			} else {
				if (p0 < this.values.length) {
					r.indices[pos] = this.indices[p0];
					r.values[pos] = this.values[p0];
					p0++;
					pos++;
					continue;
				}
				if (p1 < other.values.length) {
					r.indices[pos] = other.indices[p1];
					r.values[pos] = other.values[p1];
					p1++;
					pos++;
					continue;
				}
			}
		}
		return r;
	}

	public DenseVector plus(DenseVector other) {
		if (this.size() != other.size()) {
			throw new RuntimeException("The size of the two vectors are different.");
		}

		DenseVector r = other.clone();
		for (int i = 0; i < this.indices.length; i++) {
			r.add(this.indices[i], this.values[i]);
		}
		return r;
	}

	@Override
	public Vector plus(Vector vec) {
		if (vec instanceof DenseVector) {
			return plus((DenseVector) vec);
		} else {
			return plus((SparseVector) vec);
		}
	}

	public SparseVector minus(SparseVector other) {
		if (this.size() != other.size()) {
			throw new RuntimeException("The size of the two vectors are different.");
		}

		int totNnz = this.values.length + other.values.length;
		int p0 = 0;
		int p1 = 0;
		while (p0 < this.values.length && p1 < other.values.length) {
			if (this.indices[p0] == other.indices[p1]) {
				totNnz--;
				p0++;
				p1++;
			} else if (this.indices[p0] < other.indices[p1]) {
				p0++;
			} else {
				p1++;
			}
		}

		SparseVector r = new SparseVector(this.size());
		r.indices = new int[totNnz];
		r.values = new double[totNnz];
		p0 = p1 = 0;
		int pos = 0;
		while (pos < totNnz) {
			if (p0 < this.values.length && p1 < other.values.length) {
				if (this.indices[p0] == other.indices[p1]) {
					r.indices[pos] = this.indices[p0];
					r.values[pos] = this.values[p0] - other.values[p1];
					p0++;
					p1++;
				} else if (this.indices[p0] < other.indices[p1]) {
					r.indices[pos] = this.indices[p0];
					r.values[pos] = this.values[p0];
					p0++;
				} else {
					r.indices[pos] = other.indices[p1];
					r.values[pos] = -other.values[p1];
					p1++;
				}
				pos++;
			} else {
				if (p0 < this.values.length) {
					r.indices[pos] = this.indices[p0];
					r.values[pos] = this.values[p0];
					p0++;
					pos++;
					continue;
				}
				if (p1 < other.values.length) {
					r.indices[pos] = other.indices[p1];
					r.values[pos] = -other.values[p1];
					p1++;
					pos++;
					continue;
				}
			}
		}

		return r;
	}

	public DenseVector minus(DenseVector other) {
		if (this.size() != other.size()) {
			throw new RuntimeException("The size of the two vectors are different.");
		}

		DenseVector r = other.scale(-1.0);
		for (int i = 0; i < this.indices.length; i++) {
			r.add(this.indices[i], this.values[i]);
		}
		return r;
	}

	@Override
	public Vector minus(Vector vec) {
		if (vec instanceof DenseVector) {
			return minus((DenseVector) vec);
		} else {
			return minus((SparseVector) vec);
		}
	}

	@Override
	public SparseVector scale(double d) {
		SparseVector r = new SparseVector(this.n, this.indices, this.values);
		for (int i = 0; i < this.values.length; i++) {
			r.values[i] *= d;
		}
		return r;
	}

	@Override
	public void scaleEqual(double d) {
		for (int i = 0; i < this.values.length; i++) {
			this.values[i] *= d;
		}
	}

	public void removeZeroValues() {
		if (this.values.length != 0) {
			List<Integer> idxs = new ArrayList<>();
			for (int i = 0; i < values.length; i++) {
				if (0 != values[i]) {
					idxs.add(i);
				}
			}
			int[] newIndices = new int[idxs.size()];
			double[] newValues = new double[newIndices.length];
			for (int i = 0; i < newIndices.length; i++) {
				newIndices[i] = indices[idxs.get(i)];
				newValues[i] = values[idxs.get(i)];
			}
			this.indices = newIndices;
			this.values = newValues;
		}
	}

	public double dot(SparseVector other) {
		if (this.size() != other.size()) {
			throw new RuntimeException("the size of the two vectors are different");
		}

		double d = 0;
		int p0 = 0;
		int p1 = 0;
		while (p0 < this.values.length && p1 < other.values.length) {
			if (this.indices[p0] == other.indices[p1]) {
				d += this.values[p0] * other.values[p1];
				p0++;
				p1++;
			} else if (this.indices[p0] < other.indices[p1]) {
				p0++;
			} else {
				p1++;
			}
		}
		return d;
	}

	public double dot(DenseVector other) {
		if (this.size() != other.size()) {
			throw new RuntimeException(
				"The size of the two vectors are different: " + this.size() + " vs " + other.size());
		}
		double s = 0.;
		for (int i = 0; i < this.indices.length; i++) {
			s += this.values[i] * other.get(this.indices[i]);
		}
		return s;
	}

	@Override
	public double dot(Vector other) {
		if (other instanceof DenseVector) {
			return dot((DenseVector) other);
		} else {
			return dot((SparseVector) other);
		}
	}

	public DenseMatrix outer() {
		return this.outer(this);
	}

	public DenseMatrix outer(SparseVector other) {
		int nrows = this.size();
		int ncols = other.size();
		double[][] mat = new double[nrows][ncols];
		for (int i = 0; i < mat.length; i++) {
			Arrays.fill(mat[i], 0.);
		}
		for (int i = 0; i < this.values.length; i++) {
			for (int j = 0; j < other.values.length; j++) {
				mat[this.indices[i]][other.indices[j]] = this.values[i] * other.values[j];
			}
		}
		return new DenseMatrix(mat);
	}

	@Override
	public DenseVector toDenseVector() {
		if (n >= 0) {
			DenseVector r = new DenseVector(n);
			for (int i = 0; i < this.indices.length; i++) {
				r.set(this.indices[i], this.values[i]);
			}
			return r;
		} else {
			if (this.indices.length == 0) {
				return new DenseVector();
			} else {
				int n = this.indices[this.indices.length - 1] + 1;
				DenseVector r = new DenseVector(n);
				for (int i = 0; i < this.indices.length; i++) {
					r.set(this.indices[i], this.values[i]);
				}
				return r;
			}
		}
	}

	private class SparseVectorVectorIterator implements VectorIterator {
		private int cursor = 0;

		@Override
		public boolean hasNext() {
			return cursor < values.length;
		}

		@Override
		public void next() {
			cursor++;
		}

		@Override
		public int getIndex() {
			if (cursor >= values.length) {
				throw new RuntimeException("Iterator out of bound.");
			}
			return indices[cursor];
		}

		@Override
		public double getValue() {
			if (cursor >= values.length) {
				throw new RuntimeException("Iterator out of bound.");
			}
			return values[cursor];
		}
	}

	@Override
	public String serialize() {
		StringBuilder sbd = new StringBuilder();
		if (n > 0) {
			sbd.append("$");
			sbd.append(n);
			sbd.append("$");
		}
		if (null != indices) {
			assert (indices.length == values.length);
			for (int i = 0; i < indices.length; i++) {

				sbd.append(indices[i] + ":");
				sbd.append(values[i]);
				if (i < indices.length - 1) {
					sbd.append(",");
				}
			}
		}

		return sbd.toString();
	}

	/**
	 * Parse the sparse vector from a formatted string.
	 *
	 * @throws IllegalArgumentException If the string is of invalid format.
	 */
	public static SparseVector deserialize(String str) {
		try {
			if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)) {
				return new SparseVector();
			}

			int n = -1;
			int firstDollarPos = str.indexOf('$');
			int lastDollarPos = -1;
			if (firstDollarPos >= 0) {
				lastDollarPos = StringUtils.lastIndexOf(str, '$');
				String sizeStr = StringUtils.substring(str, firstDollarPos + 1, lastDollarPos);
				n = Integer.valueOf(sizeStr);
				if (lastDollarPos == str.length() - 1) {
					return new SparseVector(n);
				}
			}

			int numValues = StringUtils.countMatches(str, ",") + 1;
			double[] data = new double[numValues];
			int[] indices = new int[numValues];
			int startPos = lastDollarPos + 1;
			int endPos;
			for (int i = 0; i < numValues; i++) {
				endPos = StringUtils.indexOf(str, ",", startPos);
				if (endPos == -1) {
					endPos = str.length();
				}
				String valueStr = StringUtils.substring(str, startPos, endPos);
				startPos = endPos + 1;

				int colonPos = valueStr.indexOf(':');
				if (colonPos < 0) {
					throw new IllegalArgumentException("Format error.");
				}
				indices[i] = Integer.valueOf(valueStr.substring(0, colonPos).trim());
				data[i] = Double.valueOf(valueStr.substring(colonPos + 1).trim());
			}
			return new SparseVector(n, indices, data);
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format("Fail to parse sparse vector from string: \"%s\".", str), e);
		}
	}

	@Override
	public void standardizeEqual(double mean, double stdvar) {
		for (int i = 0; i < indices.length; i++) {
			values[i] -= mean;
			values[i] *= (1.0 / stdvar);
		}
	}

	@Override
	public void normalizeEqual(double p) {
		double norm = 0.0;
		if (Double.isInfinite(p)) {
			for (int i = 0; i < indices.length; i++) {
				norm = Math.max(norm, Math.abs(values[i]));
			}
		} else if (p == 1.0) {
			for (int i = 0; i < indices.length; i++) {
				norm += Math.abs(values[i]);
			}
		} else if (p == 2.0) {
			for (int i = 0; i < indices.length; i++) {
				norm += values[i] * values[i];
			}
			norm = Math.sqrt(norm);
		} else {
			for (int i = 0; i < indices.length; i++) {
				norm += Math.pow(values[i], p);
			}
			norm = Math.pow(norm, 1 / p);
		}

		for (int i = 0; i < indices.length; i++) {
			values[i] /= norm;
		}
	}

	@Override
	public VectorIterator iterator() {
		return new SparseVectorVectorIterator();
	}
}
