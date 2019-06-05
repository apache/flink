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
import java.util.regex.Pattern;

/**
 * Sparse Vector.
 */
public class SparseVector extends Vector {

	/**
	 * size of the vector.
	 */
	public int n = -1;

	/**
	 * number of nonzeros.
	 */
	public int nnz = 0;

	/**
	 * column indices.
	 */
	public int[] indices = null;

	/**
	 * column values.
	 */
	public double[] values = null;

	public SparseVector() {
		this(-1);
	}

	public SparseVector(int n) {
		this.n = n;
		this.indices = new int[0];
		this.values = new double[0];
	}

	public SparseVector(int n, Map <Integer, Double> kv) {
		int nnz = kv.size();
		int[] indices = new int[nnz];
		double[] values = new double[nnz];
		int index = 0;
		for (Integer key : kv.keySet()) {
			indices[index] = key;
			values[index++] = kv.get(key);
		}
		initialize(n, indices, values);
	}

	public SparseVector(int n, int[] indices, double[] values) {
		this();
		initialize(n, indices, values);
	}

	public static SparseVector parseKV(String kvString, String colDelimiter, String valDelimiter) {
		return parseKV(kvString, -1, colDelimiter, valDelimiter);
	}

	/**
	 * @param kvString
	 * @param n            the size of the vector. if n == -1, then the size is undetermined
	 * @param colDelimiter
	 * @param valDelimiter
	 * @return
	 */
	public static SparseVector parseKV(String kvString, int n, String colDelimiter, String valDelimiter) {

		if (null == kvString || kvString.trim().isEmpty()) {
			return new SparseVector(n);
		}

		String colDelimiterQuote = Pattern.quote(colDelimiter);
		String valDelimiterQuote = Pattern.quote(valDelimiter);

		String[] kvs = kvString.split(colDelimiterQuote);

		if (0 == kvs.length) {
			return new SparseVector(n);
		}

		final int[] indices = new int[kvs.length];
		final double[] values = new double[kvs.length];

		int count = 0;
		for (int i = 0; i < kvs.length; i++) {
			String[] kv = kvs[i].split(valDelimiterQuote);
			if (0 == kv.length || kv[0].trim().isEmpty()) {
				continue;
			}

			int key = Integer.parseInt(kv[0].trim());
			double value = 1.0;

			if (kv.length > 1 && (!kv[1].trim().isEmpty())) {
				value = Double.parseDouble(kv[1].trim());
			}

			indices[count] = key;
			values[count] = value;
			count++;
		}

		// lucky try
		boolean lucky = (count == indices.length);
		if (lucky) { // check if in order
			for (int i = 0; i < indices.length - 1; i++) {
				if (indices[i] >= indices[i + 1]) {
					lucky = false;
					break;
				}
			}
		}

		if (lucky) {
			return new SparseVector(n, indices, values);
		} else {
			TreeMap <Integer, Double> kvMap = new TreeMap <>();
			for (int i = 0; i < count; i++) {
				if (kvMap.containsKey(indices[i])) {
					if (!kvMap.get(indices[i]).equals(values[i])) {
						throw new RuntimeException("duplicated key with different values.");
					}
				} else {
					kvMap.put(indices[i], values[i]);
				}
			}

			int pos = 0;
			for (Map.Entry <Integer, Double> entry : kvMap.entrySet()) {
				Integer key = entry.getKey();
				Double value = entry.getValue();
				indices[pos] = key;
				values[pos] = value;
				pos++;
			}

			return new SparseVector(n, Arrays.copyOf(indices, kvMap.size()), Arrays.copyOf(values, kvMap.size()));
		}
	}

	public static SparseVector deserialize(String str) {
		try {
			if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)) {
				return new SparseVector();
			}

			str = StringUtils.trim(str);
			int length = -1;
			if (str.charAt(0) == '$') {
				int lastPos = StringUtils.lastIndexOf(str, '$');
				String lengthStr = StringUtils.substring(str, 1, lastPos);

				length = Integer.valueOf(lengthStr);
				str = StringUtils.substring(str, lastPos + 1);
				str = StringUtils.trim(str);
				if (str.isEmpty()) {
					return new SparseVector(length);
				}
			}

			int numValues = StringUtils.countMatches(str, ",") + 1;

			double[] data = new double[numValues];
			int[] indices = null;
			int startPos = 0;
			int endPos = -1;
			for (int i = 0; i < numValues; i++) {
				// extract the value string
				endPos = StringUtils.indexOf(str, ",", startPos);
				if (endPos == -1) {
					endPos = str.length();
				}
				String valueStr = StringUtils.substring(str, startPos, endPos);
				startPos = endPos + 1;

				if (indices == null) {
					indices = new int[numValues];
				}

				String[] kvStr = StringUtils.split(valueStr, ':');
				if (kvStr.length != 2) {
					throw new IllegalArgumentException("mismatched size of kv.");
				}
				indices[i] = Integer.valueOf(kvStr[0].trim());
				data[i] = Double.valueOf(kvStr[1].trim());
			}
			return new SparseVector(length, indices, data);

		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("fail to parse tensor \"" + str + "\"");
		}
	}

	private void initialize(int n, int[] indices, double[] values) {
		if (indices.length != values.length) {
			throw new RuntimeException("indices size and values size should be the same.");
		}
		for (int i = 0; i < indices.length; i++) {
			if (n >= 0) {
				if (indices[i] < 0 || indices[i] >= n) {
					throw new RuntimeException("Invalid index. n = " + n + " and index = " + indices[i]);
				}
			}
		}

		boolean outOfOrder = false;
		for (int i = 0; i < indices.length - 1; i++) {
			if (indices[i] >= indices[i + 1]) {
				outOfOrder = true;
				break;
			}
		}

		if (outOfOrder) {
			// sort
			Integer[] order = new Integer[indices.length];
			for (int i = 0; i < order.length; i++) {
				order[i] = i;
			}

			Arrays.sort(order, new Comparator <Integer>() {
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

			this.n = n;
			this.nnz = indices.length;
			this.indices = new int[this.nnz];
			this.values = new double[this.nnz];

			for (int i = 0; i < order.length; i++) {
				this.values[i] = values[order[i]];
				this.indices[i] = indices[order[i]];
			}

		} else {
			this.n = n;
			this.nnz = indices.length;
			this.indices = indices.clone();
			this.values = values.clone();
		}
	}

	@Override
	public SparseVector clone() {
		SparseVector vec = new SparseVector(this.n);
		vec.indices = this.indices.clone();
		vec.values = this.values.clone();
		vec.nnz = this.nnz;
		return vec;
	}

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
		if (n < 0) {
			throw new RuntimeException("the vector size has not been set!");
		}
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

	public SparseVector setSize(int n) {
		this.n = n;
		return this;
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
			this.nnz = newValues.length;
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
			this.nnz = newValues.length;
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
	public SparseVector slice(int[] indexes) {
		TreeMap <Integer, Double> map = new TreeMap <>();
		for (int i = 0; i < indexes.length; ++i) {
			if (n != -1 && indexes[i] >= n) {
				throw new RuntimeException("VectorSlicer: indices is larger than vec size.");
			}
			if (get(indexes[i]) != 0) {
				map.put(i, get(indexes[i]));
			}
		}
		return new SparseVector(indexes.length, map);
	}

	public SparseVector minus(SparseVector other) {
		if (this.size() != other.size()) {
			throw new RuntimeException("the size of the two vectors are different");
		}

		int totNnz = this.nnz + other.nnz;
		int p0 = 0;
		int p1 = 0;
		while (p0 < this.nnz && p1 < other.nnz) {
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
		r.nnz = totNnz;
		p0 = p1 = 0;
		int pos = 0;
		while (pos < totNnz) {
			if (p0 < this.nnz && p1 < other.nnz) {
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
				if (p0 < this.nnz) {
					r.indices[pos] = this.indices[p0];
					r.values[pos] = this.values[p0];
					p0++;
					pos++;
					continue;
				}
				if (p1 < other.nnz) {
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
			throw new RuntimeException("the size of the two vectors are different");
		}

		DenseVector r = other.scale(-1.0);
		for (int i = 0; i < this.indices.length; i++) {
			r.add(this.indices[i], this.values[i]);
		}
		return r;
	}

	public SparseVector plus(SparseVector other) {
		if (this.size() != other.size()) {
			throw new RuntimeException("the size of the two vectors are different");
		}

		int totNnz = this.nnz + other.nnz;
		int p0 = 0;
		int p1 = 0;
		while (p0 < this.nnz && p1 < other.nnz) {
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
		r.nnz = totNnz;
		p0 = p1 = 0;
		int pos = 0;
		while (pos < totNnz) {
			if (p0 < this.nnz && p1 < other.nnz) {
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
				if (p0 < this.nnz) {
					r.indices[pos] = this.indices[p0];
					r.values[pos] = this.values[p0];
					p0++;
					pos++;
					continue;
				}
				if (p1 < other.nnz) {
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
			throw new RuntimeException("the size of the two vectors are different");
		}

		DenseVector r = new DenseVector(other);
		for (int i = 0; i < this.indices.length; i++) {
			r.add(this.indices[i], this.values[i]);
		}
		return r;
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
	public Vector scaleEqual(double d) {
		for (int i = 0; i < this.values.length; i++) {
			this.values[i] *= d;
		}
		return this;
	}

	public void deleteValueZero() {
		if (this.nnz != 0) {
			List <Integer> idxs = new ArrayList <>();
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

			this.nnz = newIndices.length;
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
		while (p0 < this.nnz && p1 < other.nnz) {
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
				"the size of the two vectors are different: " + this.size() + " vs " + other.size());
		}
		double s = 0.;
		for (int i = 0; i < this.indices.length; i++) {
			s += this.values[i] * other.get(this.indices[i]);
		}
		return s;
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
		for (int i = 0; i < this.nnz; i++) {
			for (int j = 0; j < other.nnz; j++) {
				mat[this.indices[i]][other.indices[j]] = this.values[i] * other.values[j];
			}
		}
		return new DenseMatrix(mat);
	}

	@Override
	public DenseVector toDenseVector() {
		if (nnz > 0) {
			int size = n;
			if (size < 0) {
				size = indices[indices.length - 1] + 1;
			}
			DenseVector r = new DenseVector(size);
			for (int i = 0; i < this.indices.length; i++) {
				r.set(this.indices[i], this.values[i]);
			}
			return r;
		}
		return new DenseVector();
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

	@Override
	public Vector standard(double mean, double stdvar) {
		for (int i = 0; i < indices.length; i++) {
			values[i] -= mean;
			values[i] *= (1.0 / stdvar);
		}
		return this;
	}

	@Override
	public Vector normalize(double p) {
		double norm = 0.0;
		if (Double.valueOf(p).equals(-1.0)) {
			for (int i = 0; i < indices.length; i++) {
				norm = Math.max(norm, Math.abs(values[i]));
			}
		} else if (Double.valueOf(p).equals(1.0)) {
			for (int i = 0; i < indices.length; i++) {
				norm += Math.abs(values[i]);
			}
		} else if (Double.valueOf(p).equals(2.0)) {
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

		return this;
	}

	@Override
	public VectorIterator iterator() {
		return new SparseVectorVectorIterator();
	}

	private class SparseVectorVectorIterator implements VectorIterator {
		private int cursor = 0;

		@Override
		public boolean hasNext() {
			return cursor < nnz;
		}

		@Override
		public void next() {
			cursor++;
		}

		@Override
		public int getIndex() {
			if (cursor >= nnz) {
				throw new RuntimeException("iterator out of bound");
			}
			return indices[cursor];
		}

		@Override
		public double getValue() {
			if (cursor >= nnz) {
				throw new RuntimeException("iterator out of bound");
			}
			return values[cursor];
		}
	}
}
