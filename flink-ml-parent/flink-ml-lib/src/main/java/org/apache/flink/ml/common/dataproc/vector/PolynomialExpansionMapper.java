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

package org.apache.flink.ml.common.dataproc.vector;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.linalg.Vector;
import org.apache.flink.ml.common.mapper.SISOMapper;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.dataproc.vector.VectorPolynomialExpandParams;
import org.apache.flink.table.api.TableSchema;

import org.apache.commons.math3.util.ArithmeticUtils;

/**
 * Polynomial expansion mapper will map a vector to a longer vector by polynomial transform.
 */
public class PolynomialExpansionMapper extends SISOMapper {
	/**
	 * the degree of the expanded polynomial.
	 */
	private int degree;

	private static final int CONSTANT = 61;

	public PolynomialExpansionMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.degree = this.params.get(VectorPolynomialExpandParams.DEGREE);
	}

	/**
	 * calculate the length of the expended polynomial.
	 *
	 * @param num    the item number of the input polynomial.
	 * @param degree the degree of the polynomial.
	 * @return the polynomial size.
	 */
	@VisibleForTesting
	static int getPolySize(int num, int degree) {
		if (num == 0) {
			return 1;
		}
		if (num == 1 || degree == 1) {
			return num + degree;
		}
		if (degree > num) {
			return getPolySize(degree, num);
		}
		long res = 1;
		int i = num + 1;
		int j;
		if (num + degree < CONSTANT) {
			for (j = 1; j <= degree; ++j) {
				res = res * i / j;
				++i;
			}
		} else {
			int depth;
			for (j = 1; j <= degree; ++j) {
				depth = ArithmeticUtils.gcd(i, j);
				res = ArithmeticUtils.mulAndCheck(res / (j / depth), i / depth);
				++i;
			}
		}
		if (res > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("The expended polynomial size is too large.");
		}
		return (int) res;

	}

	@Override
	protected TypeInformation initOutputColType() {
		return VectorTypes.VECTOR;
	}

	@Override
	protected Object map(Object input) {
		Vector vec = (Vector) input;
		if (null == vec) {
			return null;
		}

		if (vec instanceof SparseVector) {
			return sparsePE((SparseVector) vec, degree);
		} else {
			return densePE((DenseVector) vec, degree);
		}
	}

	/**
	 * dense vector polynomial expansion, if vector is dense, then this inner function will be invoked.
	 *
	 * @param vec    input vector
	 * @param degree degree of polynomial expansion
	 * @return output vector
	 */
	private DenseVector densePE(DenseVector vec, int degree) {
		int size = vec.size();
		double[] retVals = new double[getPolySize(size, degree) - 1];
		expandDense(vec.getData(), size - 1, degree, 1.0, retVals, -1);
		return new DenseVector(retVals);
	}

	/**
	 * sparse vector polynomial expansion, if vector is sparse, then this inner function will be invoked.
	 *
	 * @param vec    input vector.
	 * @param degree degree of the expended polynomial.
	 * @return output vector.
	 */
	private SparseVector sparsePE(SparseVector vec, int degree) {
		int[] indices = vec.getIndices();
		double[] values = vec.getValues();
		int size = vec.size();
		int nnz = vec.getValues().length;
		int nnzPolySize = getPolySize(nnz, degree);
		Tuple2<Integer, int[]> polyIndices = Tuple2.of(0, new int[nnzPolySize - 1]);
		Tuple2<Integer, double[]> polyValues = Tuple2.of(0, new double[nnzPolySize - 1]);

		expandSparse(indices, values, nnz - 1, size - 1, degree, 1.0, polyIndices, polyValues, -1);
		return new SparseVector(getPolySize(size, degree) - 1, polyIndices.f1, polyValues.f1);
	}

	/**
	 * dense vector polynomial expansion function.
	 *
	 * @param values     the values of the input vector.
	 * @param lastIdx    the id of the last value in vector.
	 * @param degree     the degree of the expanded polynomial.
	 * @param factor     the factor coefficient.
	 * @param retValues  write values in it and finally return it.
	 * @param curPolyIdx the current polynomial index.
	 */
	private int expandDense(double[] values, int lastIdx, int degree, double factor, double[] retValues,
							int curPolyIdx) {
		if (!Double.valueOf(factor).equals(0.0)) {
			if (degree == 0 || lastIdx < 0) {
				if (curPolyIdx >= 0) {
					retValues[curPolyIdx] = factor;
				}
			} else {
				double v = values[lastIdx];
				int newLastIdx = lastIdx - 1;
				double alpha = factor;
				int i = 0;
				int curStart = curPolyIdx;
				while (i <= degree && Math.abs(alpha) > 0.0) {
					curStart = expandDense(values, newLastIdx, degree - i, alpha, retValues, curStart);
					i += 1;
					alpha *= v;
				}
			}
		}
		return curPolyIdx + getPolySize(lastIdx + 1, degree);
	}

	/**
	 * sparse vector polynomial expansion function.
	 *
	 * @param indices        the indices of the input sparse vector.
	 * @param values         the values of the input sparse vector.
	 * @param lastIdx        the id of the last value in vector.
	 * @param lastFeatureIdx the id of the last value in the temp iteration.
	 * @param degree         the degree of the expended polynomial.
	 * @param factor         the factor coefficient.
	 * @param polyIndices    write indices of the output sparse vector.
	 * @param polyValues     writh values of the output sparse vector.
	 * @param curPolyIdx     the current polynomial index.
	 */
	private int expandSparse(
		int[] indices, double[] values, int lastIdx, int lastFeatureIdx, int degree,
		double factor, Tuple2<Integer, int[]> polyIndices,
		Tuple2<Integer, double[]> polyValues, int curPolyIdx) {
		if (!Double.valueOf(factor).equals(0.0)) {
			if (degree == 0 || lastIdx < 0) {
				if (curPolyIdx >= 0) {
					polyIndices.f1[polyIndices.f0] = curPolyIdx;
					polyValues.f1[polyValues.f0] = factor;
					polyIndices.f0++;
					polyValues.f0++;
				}
			} else {
				double v = values[lastIdx];
				int lastIdx1 = lastIdx - 1;
				int lastFeatureIdx1 = indices[lastIdx] - 1;
				double alpha = factor;
				int curStart = curPolyIdx;
				int i = 0;
				while (i <= degree && Math.abs(alpha) > 0.0) {
					curStart = expandSparse(indices, values, lastIdx1, lastFeatureIdx1, degree - i, alpha,
						polyIndices, polyValues, curStart);
					i += 1;
					alpha *= v;
				}
			}
		}
		return curPolyIdx + getPolySize(lastFeatureIdx + 1, degree);
	}
}
