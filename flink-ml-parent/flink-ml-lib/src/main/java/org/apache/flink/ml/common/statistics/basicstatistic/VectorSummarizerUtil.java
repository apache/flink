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

package org.apache.flink.ml.common.statistics.basicstatistic;

import org.apache.flink.ml.common.linalg.DenseMatrix;

import java.util.Map;

/**
 * Utility class for the operations on {@link BaseVectorSummarizer} and its subclasses.
 */
public class VectorSummarizerUtil {
	/**
	 * left merge right, return a new summarizer. left and right will be changed.
	 */
	public static BaseVectorSummarizer merge(BaseVectorSummarizer left, BaseVectorSummarizer right) {
		if (left instanceof SparseVectorSummarizer && right instanceof SparseVectorSummarizer) {
			return merge((SparseVectorSummarizer) left, (SparseVectorSummarizer) right);
		} else if (left instanceof SparseVectorSummarizer && right instanceof DenseVectorSummarizer) {
			return merge((SparseVectorSummarizer) left, (DenseVectorSummarizer) right);
		} else if (left instanceof DenseVectorSummarizer && right instanceof SparseVectorSummarizer) {
			return merge((DenseVectorSummarizer) left, (SparseVectorSummarizer) right);
		} else if (left instanceof DenseVectorSummarizer && right instanceof DenseVectorSummarizer) {
			return merge((DenseVectorSummarizer) left, (DenseVectorSummarizer) right);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * merge left and right, return a new summarizer. left and right will be changed.
	 */
	private static DenseVectorSummarizer merge(DenseVectorSummarizer left, DenseVectorSummarizer right) {
		if (right.count == 0) {
			return left;
		}

		if (left.count == 0) {
			left.count = right.count;
			left.sum = right.sum.clone();
			left.squareSum = right.squareSum.clone();
			left.normL1 = right.normL1.clone();
			left.min = right.min.clone();
			left.max = right.max.clone();
			left.numNonZero = right.numNonZero.clone();

			if (right.outerProduct != null) {
				left.outerProduct = right.outerProduct.clone();
			}

			return left;
		}

		int n = left.sum.size();
		int nSrt = right.sum.size();

		if (n >= nSrt) {
			left.count += right.count;
			for (int i = 0; i < nSrt; i++) {
				left.sum.add(i, right.sum.get(i));
				left.squareSum.add(i, right.squareSum.get(i));
				left.normL1.add(i, right.normL1.get(i));
				left.min.set(i, Math.min(right.min.get(i), right.min.get(i)));
				left.max.set(i, Math.min(right.max.get(i), right.max.get(i)));
				left.numNonZero.add(i, right.numNonZero.get(i));
			}

			if (left.outerProduct != null && right.outerProduct != null) {
				for (int i = 0; i < nSrt; i++) {
					for (int j = 0; j < nSrt; j++) {
						left.outerProduct.add(i, j, right.outerProduct.get(i, j));
					}
				}
			} else if (left.outerProduct == null && right.outerProduct != null) {
				left.outerProduct = right.outerProduct.clone();
			}
			return left;
		} else {
			DenseVectorSummarizer clonedSrt = right.copy();
			return merge(clonedSrt, left);
		}
	}

	/**
	 * merge left and right, return a new summarizer. left and right will be changed.
	 */
	private static SparseVectorSummarizer merge(DenseVectorSummarizer left, SparseVectorSummarizer right) {
		return merge(right, left);
	}

	/**
	 * merge left and right, return a new summarizer. left and right will be changed.
	 */
	private static SparseVectorSummarizer merge(SparseVectorSummarizer left, DenseVectorSummarizer right) {
		if (right.count != 0) {
			left.count += right.count;
			for (int i = 0; i < right.sum.size(); i++) {
				VectorStatCol vsc = new VectorStatCol();
				vsc.numNonZero = (long) right.numNonZero.get(i);
				vsc.sum = right.sum.get(i);
				vsc.squareSum = right.squareSum.get(i);
				vsc.min = right.min.get(i);
				vsc.max = right.max.get(i);
				vsc.normL1 = right.normL1.get(i);
				if (left.cols.containsKey(i)) {
					left.cols.get(i).merge(vsc);
				} else {
					left.cols.put(i, vsc);
				}
			}
		}

		if (left.outerProduct != null && right.outerProduct != null) {
			int size = right.outerProduct.numRows();
			if (right.outerProduct.numRows() > left.outerProduct.numRows()) {
				DenseMatrix dpNew = DenseMatrix.zeros(size, size);
				left.outerProduct = plusEqual(dpNew, left.outerProduct);
			}
			left.outerProduct = plusEqual(left.outerProduct, right.outerProduct);
		} else if (left.outerProduct == null && right.outerProduct != null) {
			left.outerProduct = right.outerProduct.clone();
		}

		return left;
	}

	/**
	 * merge left and right, return a new summarizer. left and right will be changed.
	 */
	private static SparseVectorSummarizer merge(SparseVectorSummarizer left, SparseVectorSummarizer right) {
		left.count += right.count;
		left.colNum = Math.max(right.colNum, left.colNum);
		for (Map.Entry<Integer, VectorStatCol> entry : right.cols.entrySet()) {
			int idx = entry.getKey();
			if (left.cols.containsKey(idx)) {
				left.cols.get(idx).merge(entry.getValue());
			} else {
				left.cols.put(idx, entry.getValue());
			}
		}

		if (left.outerProduct != null && right.outerProduct != null) {
			int size = right.outerProduct.numRows();
			if (right.outerProduct.numRows() > left.outerProduct.numRows()) {
				DenseMatrix dpNew = DenseMatrix.zeros(size, size);
				left.outerProduct = plusEqual(dpNew, left.outerProduct);
			}
			left.outerProduct = plusEqual(left.outerProduct, right.outerProduct);
		} else if (left.outerProduct == null && right.outerProduct != null) {
			left.outerProduct = right.outerProduct.clone();
		}

		return left;
	}

	/**
	 * return left + right
	 * row and col of right matrix is  equal with or less than left matrix.
	 * it will change left, right will not be change.
	 */
	static DenseMatrix plusEqual(DenseMatrix left, DenseMatrix right) {
		for (int i = 0; i < right.numRows(); i++) {
			for (int j = 0; j < right.numCols(); j++) {
				left.add(i, j, right.get(i, j));
			}
		}
		return left;
	}
}
