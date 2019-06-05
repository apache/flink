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

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * Base class of vector.
 */
public abstract class Vector implements Serializable {
	private static final long serialVersionUID = 7690668399245109611L;

	public Vector() {
	}

	public static DenseVector dense(String str) {
		return DenseVector.deserialize(str);
	}

	public static SparseVector sparse(String str) {
		return SparseVector.deserialize(str);
	}

	public static Tuple2 <int[], float[]> parseSparseTensor(String str) {
		int numValues = 1;
		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) == ',') {
				numValues++;
			}
		}
		int[] indices = new int[numValues];
		float[] values = new float[numValues];

		int startPos = StringUtils.lastIndexOf(str, '$') + 1;
		int endPos = -1;
		int delimiterPos;

		for (int i = 0; i < numValues; i++) {
			// extract the value string
			endPos = StringUtils.indexOf(str, ',', startPos);
			if (endPos == -1) {
				endPos = str.length();
			}
			delimiterPos = StringUtils.indexOf(str, ':', startPos);
			if (delimiterPos == -1) {
				throw new RuntimeException("invalid data: " + str);
			}
			indices[i] = Integer.valueOf(StringUtils.substring(str, startPos, delimiterPos));
			values[i] = Float.valueOf(StringUtils.substring(str, delimiterPos + 1, endPos));
			startPos = endPos + 1;
		}

		return Tuple2.of(indices, values);
	}

	public static boolean isSparse(String str) {
		if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)) {
			return true;
		}
		return StringUtils.indexOf(str, ':') != -1 || StringUtils.indexOf(str, "$") != -1;
	}

	public static Vector deserialize(String str) {
		Vector vec;
		if (isSparse(str)) {
			vec = Vector.sparse(str);
		} else {
			vec = Vector.dense(str);
		}
		return vec;
	}

	public static Vector plus(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
			return ((DenseVector) vec1).plus((DenseVector) vec2);
		} else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
			return ((SparseVector) vec1).plus((SparseVector) vec2);
		} else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
			return ((SparseVector) vec1).plus((DenseVector) vec2);
		} else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
			return ((SparseVector) vec2).plus((DenseVector) vec1);
		} else {
			throw new RuntimeException("Not implemented yet!");
		}
	}

	public static Vector minus(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
			return ((DenseVector) vec1).minus((DenseVector) vec2);
		} else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
			return ((SparseVector) vec1).minus((SparseVector) vec2);
		} else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
			return ((SparseVector) vec1).minus((DenseVector) vec2);
		} else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
			return ((SparseVector) vec2).scale(-1.0).plus((DenseVector) vec1);
		} else {
			throw new RuntimeException("Not implemented yet!");
		}
	}

	public static double dot(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
			return ((DenseVector) vec1).dot((DenseVector) vec2);
		} else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
			return ((SparseVector) vec2).dot((DenseVector) vec1);
		} else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
			return ((SparseVector) vec1).dot((DenseVector) vec2);
		} else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
			return ((SparseVector) vec2).dot((SparseVector) vec1);
		} else {
			throw new RuntimeException("Not implemented yet!");
		}
	}

	public static double sumAbsDiff(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
			return applySum((DenseVector) vec1, (DenseVector) vec2, (a, b) -> Math.abs(a - b));
		} else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
			return applySum((SparseVector) vec1, (SparseVector) vec2, (a, b) -> Math.abs(a - b));
		} else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
			return applySum((SparseVector) vec1, (DenseVector) vec2, (a, b) -> Math.abs(a - b));
		} else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
			return applySum((DenseVector) vec1, (SparseVector) vec2, (a, b) -> Math.abs(a - b));
		} else {
			throw new RuntimeException("Not implemented yet!");
		}
	}

	public static double sumSquaredDiff(Vector vec1, Vector vec2) {
		if (vec1 instanceof DenseVector && vec2 instanceof DenseVector) {
			return applySum((DenseVector) vec1, (DenseVector) vec2, (a, b) -> (a - b) * (a - b));
		} else if (vec1 instanceof SparseVector && vec2 instanceof SparseVector) {
			return applySum((SparseVector) vec1, (SparseVector) vec2, (a, b) -> (a - b) * (a - b));
		} else if (vec1 instanceof SparseVector && vec2 instanceof DenseVector) {
			return applySum((SparseVector) vec1, (DenseVector) vec2, (a, b) -> (a - b) * (a - b));
		} else if (vec1 instanceof DenseVector && vec2 instanceof SparseVector) {
			return applySum((DenseVector) vec1, (SparseVector) vec2, (a, b) -> (a - b) * (a - b));
		} else {
			throw new RuntimeException("Not implemented yet!");
		}
	}

	/**
	 * Compute element wise sum.
	 * \sum_i func(x1_i, x2_i)
	 */
	public static double applySum(DenseVector x1, DenseVector x2, BinaryOp func) {
		assert x1.size() == x2.size();
		double s = 0.;
		for (int i = 0; i < x1.data.length; i++) {
			s += func.f(x1.data[i], x2.data[i]);
		}
		return s;
	}

	public static double applySum(SparseVector x1, SparseVector x2, BinaryOp func) {
		double s = 0.;
		int p1 = 0;
		int p2 = 0;
		int nnz1 = x1.indices.length;
		int nnz2 = x2.indices.length;
		while (p1 < nnz1 || p2 < nnz2) {
			if (p1 < nnz1 && p2 < nnz2) {
				if (x1.indices[p1] == x2.indices[p2]) {
					s += func.f(x1.values[p1], x2.values[p2]);
					p1++;
					p2++;
				} else if (x1.indices[p1] < x2.indices[p2]) {
					s += func.f(x1.values[p1], 0.);
					p1++;
				} else {
					s += func.f(0., x2.values[p2]);
					p2++;
				}
			} else {
				if (p1 < nnz1) {
					s += func.f(x1.values[p1], 0.);
					p1++;
				} else { // p2 < nnz2
					s += func.f(0., x2.values[p2]);
					p2++;
				}
			}
		}
		return s;
	}

	public static double applySum(DenseVector x1, SparseVector x2, BinaryOp func) {
		assert x1.size() == x2.size();
		double s = 0.;
		int p2 = 0;
		int nnz2 = x2.indices.length;
		for (int i = 0; i < x1.data.length; i++) {
			if (p2 < nnz2 && x2.indices[p2] == i) {
				s += func.f(x1.data[i], x2.values[p2]);
				p2++;
			} else {
				s += func.f(x1.data[i], 0.);
			}
		}
		return s;
	}

	public static double applySum(SparseVector x1, DenseVector x2, BinaryOp func) {
		assert x1.size() == x2.size();
		double s = 0.;
		int p1 = 0;
		int nnz1 = x1.indices.length;
		for (int i = 0; i < x2.data.length; i++) {
			if (p1 < nnz1 && x1.indices[p1] == i) {
				s += func.f(x1.values[p1], x2.data[i]);
				p1++;
			} else {
				s += func.f(0., x2.data[i]);
			}
		}
		return s;
	}

	public abstract int size();

	public abstract double get(int i);

	public abstract void set(int i, double val);

	public abstract void add(int i, double val);

	public abstract double normL1();

	public abstract double normInf();

	public abstract double normL2();

	public abstract double normL2Square();

	public abstract Vector scale(double d);

	public abstract Vector scaleEqual(double d);

	public abstract VectorIterator iterator();

	public abstract String serialize();


    /* ---------------------------------------------------
	 * Methods of customized element wise operations
     * --------------------------------------------------- */

	public abstract Vector normalize(double exp);

	public abstract Vector standard(double mean, double stdvar);

	public abstract Vector slice(int[] indexes);

	public DenseVector toDenseVector() {
		if (this instanceof DenseVector) {
			return (DenseVector) this;
		} else {
			return ((SparseVector) this).toDenseVector();
		}
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
