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

import java.io.Serializable;

/**
 * The Vector class defines some common methods for both DenseVector and
 * SparseVector.
 */
public abstract class Vector implements Serializable {

	/**
	 * Parse a DenseVector from a formatted string.
	 */
	public static DenseVector dense(String str) {
		return DenseVector.deserialize(str);
	}

	/**
	 * Parse a SparseVector from a formatted string.
	 */
	public static SparseVector sparse(String str) {
		return SparseVector.deserialize(str);
	}

	/**
	 * To check whether the formatted string represents a SparseVector.
	 */
	public static boolean isSparse(String str) {
		if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)) {
			return true;
		}
		return StringUtils.indexOf(str, ':') != -1 || StringUtils.indexOf(str, "$") != -1;
	}

	/**
	 * Parse the tensor from a formatted string.
	 */
	public static Vector deserialize(String str) {
		Vector vec;
		if (isSparse(str)) {
			vec = Vector.sparse(str);
		} else {
			vec = Vector.dense(str);
		}
		return vec;
	}

	/**
	 * Plus two vectors and create a new vector to store the result.
	 */
	public static Vector plus(Vector vec1, Vector vec2) {
		return vec1.plus(vec2);
	}

	/**
	 * Minus two vectors and create a new vector to store the result.
	 */
	public static Vector minus(Vector vec1, Vector vec2) {
		return vec1.minus(vec2);
	}

	/**
	 * Compute the dot product of two vectors.
	 */
	public static double dot(Vector vec1, Vector vec2) {
		return vec1.dot(vec2);
	}

	/**
	 * Compute || vec1 - vec2 ||_1    .
	 */
	public static double sumAbsDiff(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector) {
			if (vec2 instanceof DenseVector) {
				return applySum((DenseVector) vec1, (DenseVector) vec2, (a, b) -> Math.abs(a - b));
			} else {
				return applySum((DenseVector) vec1, (SparseVector) vec2, (a, b) -> Math.abs(a - b));
			}
		} else {
			if (vec2 instanceof DenseVector) {
				return applySum((SparseVector) vec1, (DenseVector) vec2, (a, b) -> Math.abs(a - b));
			} else {
				return applySum((SparseVector) vec1, (SparseVector) vec2, (a, b) -> Math.abs(a - b));
			}
		}
	}

	/**
	 * Compute || vec1 - vec2 ||_2^2   .
	 */
	public static double sumSquaredDiff(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector) {
			if (vec2 instanceof DenseVector) {
				return applySum((DenseVector) vec1, (DenseVector) vec2, (a, b) -> (a - b) * (a - b));
			} else {
				return applySum((DenseVector) vec1, (SparseVector) vec2, (a, b) -> (a - b) * (a - b));
			}
		} else {
			if (vec2 instanceof DenseVector) {
				return applySum((SparseVector) vec1, (DenseVector) vec2, (a, b) -> (a - b) * (a - b));
			} else {
				return applySum((SparseVector) vec1, (SparseVector) vec2, (a, b) -> (a - b) * (a - b));
			}
		}
	}

	/**
	 * Compute element wise sum between two DenseVector.
	 * \sum_i func(x1_i, x2_i)
	 */
	public static double applySum(DenseVector x1, DenseVector x2, BinaryOp func) {
		assert x1.size() == x2.size();
		double[] x1data = x1.getData();
		double[] x2data = x2.getData();
		double s = 0.;
		for (int i = 0; i < x1data.length; i++) {
			s += func.f(x1data[i], x2data[i]);
		}
		return s;
	}

	/**
	 * Compute element wise sum between two SparseVector.
	 * \sum_i func(x1_i, x2_i)
	 */
	public static double applySum(SparseVector x1, SparseVector x2, BinaryOp func) {
		double s = 0.;
		int p1 = 0;
		int p2 = 0;
		int[] x1Indices = x1.getIndices();
		double[] x1Values = x1.getValues();
		int[] x2Indices = x2.getIndices();
		double[] x2Values = x2.getValues();
		int nnz1 = x1Indices.length;
		int nnz2 = x2Indices.length;
		while (p1 < nnz1 || p2 < nnz2) {
			if (p1 < nnz1 && p2 < nnz2) {
				if (x1Indices[p1] == x2Indices[p2]) {
					s += func.f(x1Values[p1], x2Values[p2]);
					p1++;
					p2++;
				} else if (x1Indices[p1] < x2Indices[p2]) {
					s += func.f(x1Values[p1], 0.);
					p1++;
				} else {
					s += func.f(0., x2Values[p2]);
					p2++;
				}
			} else {
				if (p1 < nnz1) {
					s += func.f(x1Values[p1], 0.);
					p1++;
				} else { // p2 < nnz2
					s += func.f(0., x2Values[p2]);
					p2++;
				}
			}
		}
		return s;
	}

	/**
	 * Compute element wise sum between a DenseVector and a SparseVector.
	 * \sum_i func(x1_i, x2_i)
	 */
	public static double applySum(DenseVector x1, SparseVector x2, BinaryOp func) {
		assert x1.size() == x2.size();
		double s = 0.;
		int p2 = 0;
		int[] x2Indices = x2.getIndices();
		double[] x2Values = x2.getValues();
		int nnz2 = x2Indices.length;
		double[] x1data = x1.getData();
		for (int i = 0; i < x1data.length; i++) {
			if (p2 < nnz2 && x2Indices[p2] == i) {
				s += func.f(x1data[i], x2Values[p2]);
				p2++;
			} else {
				s += func.f(x1data[i], 0.);
			}
		}
		return s;
	}

	/**
	 * Compute element wise sum between a SparseVector and a DenseVector.
	 * \sum_i func(x1_i, x2_i)
	 */
	public static double applySum(SparseVector x1, DenseVector x2, BinaryOp func) {
		assert x1.size() == x2.size();
		double s = 0.;
		int p1 = 0;
		int[] x1Indices = x1.getIndices();
		double[] x1Values = x1.getValues();
		int nnz1 = x1Indices.length;
		double[] x2data = x2.getData();
		for (int i = 0; i < x2data.length; i++) {
			if (p1 < nnz1 && x1Indices[p1] == i) {
				s += func.f(x1Values[p1], x2data[i]);
				p1++;
			} else {
				s += func.f(0., x2data[i]);
			}
		}
		return s;
	}

	/**
	 * Get the size of the vector.
	 */
	public abstract int size();

	/**
	 * Get the i-th element of the vector.
	 */
	public abstract double get(int i);

	/**
	 * Set the i-th element of the vector to value "val".
	 */
	public abstract void set(int i, double val);

	/**
	 * Add the i-th element of the vector by value "val".
	 */
	public abstract void add(int i, double val);

	/**
	 * Return the L1 norm of the vector.
	 */
	public abstract double normL1();

	/**
	 * Return the Inf norm of the vector.
	 */
	public abstract double normInf();

	/**
	 * Return the L2 norm of the vector.
	 */
	public abstract double normL2();

	/**
	 * Return the square of L2 norm of the vector.
	 */
	public abstract double normL2Square();

	/**
	 * Scale the vector by value "v" and create a new vector to store the result.
	 */
	public abstract Vector scale(double v);

	/**
	 * Scale the vector by value "v".
	 */
	public abstract void scaleEqual(double v);

	/**
	 * Normalize the vector.
	 */
	public abstract void normalizeEqual(double p);

	/**
	 * Standardize the vector.
	 */
	public abstract void standardizeEqual(double mean, double stdvar);

	/**
	 * Create a new vector by adding an element to the head of the vector.
	 */
	public abstract Vector prefix(double v);

	/**
	 * Create a new vector by adding an element to the end of the vector.
	 */
	public abstract Vector append(double v);

	/**
	 * Create a new vector by plussing another vector.
	 */
	public abstract Vector plus(Vector vec);

	/**
	 * Create a new vector by subtracting  another vector.
	 */
	public abstract Vector minus(Vector vec);

	/**
	 * Compute the dot product with another vector.
	 */
	public abstract double dot(Vector vec);

	/**
	 * Get the iterator of the vector.
	 */
	public abstract VectorIterator iterator();

	/**
	 * Serialize the vector to a string.
	 */
	public abstract String serialize();

	/**
	 * Slice the vector.
	 */
	public abstract Vector slice(int[] indexes);

	/**
	 * Convert the vector to DenseVector.
	 */
	public DenseVector toDenseVector() {
		if (this instanceof DenseVector) {
			return (DenseVector) this;
		} else {
			return ((SparseVector) this).toDenseVector();
		}
	}

}
