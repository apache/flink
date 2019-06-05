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

import org.apache.flink.ml.common.matrix.DenseMatrix;
import org.apache.flink.ml.common.matrix.DenseVector;
import org.apache.flink.ml.common.matrix.Vector;

/**
 * It is summarizer of dense vector.
 * It uses DenseVector to store median result.
 */
public class DenseVectorSummarizer extends BaseVectorSummarizer {

	public DenseVector sum; // sum or weight sum
	public DenseVector sum2; //sum2 or weight sum2
	public DenseVector min;
	public DenseVector max;
	public DenseVector normL1;

	public DenseVectorSummarizer() {
		this.bCov = false;
	}

	public DenseVectorSummarizer(boolean bCov) {
		this.bCov = bCov;
	}

	@Override
	public BaseVectorSummarizer visit(Vector vec) {
		if (vec instanceof DenseVector) {
			DenseVector dv = (DenseVector) vec;
			DenseVector dvc = dv.clone();

			int n = dv.size();

			if (count == 0) {
				init(n);
			}

			count++;

			sum = VectorUtil.plusEqual(sum, dv);
			normL1 = VectorUtil.plusNormL1(normL1, dv);
			sum2 = VectorUtil.plusSum2(sum2, dv);
			min = VectorUtil.minEqual(min, dv);
			max = VectorUtil.maxEqual(max, dv);

			if (bCov) {
				if (dotProduction == null) {
					dotProduction = DenseMatrix.zeros(n, n);
				} else {
					if (dv.size() > dotProduction.numRows()) {
						DenseMatrix dpNew = DenseMatrix.zeros(n, n);
						dotProduction = VectorUtil.plusEqual(dpNew, dotProduction);
					}
				}

				for (int i = 0; i < dv.size(); i++) {
					double val = dv.get(i);
					for (int j = 0; j < dv.size(); j++) {
						dotProduction.add(i, j, val * dv.get(j));
					}
				}
			}
			return this;
		} else {
			SparseVectorSummarizer sparseSrt = new SparseVectorSummarizer(bCov);
			sparseSrt.visit(vec);
			sparseSrt.merge(this);
			return sparseSrt;
		}
	}

	private void init(int n) {
		sum = new DenseVector(n);
		sum2 = new DenseVector(n);
		normL1 = new DenseVector(n);

		double[] minVals = new double[n];
		for (int i = 0; i < n; i++) {
			minVals[i] = Double.MAX_VALUE;
		}
		min = new DenseVector(minVals);

		double[] maxVals = new double[n];
		for (int i = 0; i < n; i++) {
			maxVals[i] = Double.MIN_VALUE;
		}
		max = new DenseVector(maxVals);
	}

	@Override
	public BaseVectorSummarizer visit(Vector vec, double weight) {
		if (vec instanceof DenseVector) {
			DenseVector dv = (DenseVector) vec;
			DenseVector dvc = dv.clone();
			int n = dv.size();

			if (count == 0) {
				sum = new DenseVector(n);
				sum2 = new DenseVector(n);
				normL1 = new DenseVector(n);

				double[] minVals = new double[n];
				for (int i = 0; i < n; i++) {
					minVals[i] = Double.MAX_VALUE;
				}
				min = new DenseVector(minVals);

				double[] maxVals = new double[n];
				for (int i = 0; i < n; i++) {
					maxVals[i] = Double.MIN_VALUE;
				}
				max = new DenseVector(maxVals);
			}

			count++;

			sum = VectorUtil.plusEqual(sum, dv.scale(weight));

			dvc = VectorUtil.normL1Equal(dvc.scale(weight));

			normL1 = VectorUtil.plusEqual(normL1, dvc);
			sum2 = VectorUtil.plusEqual(sum2, dv.timeEqual(dv).scale(weight));
			min = VectorUtil.minEqual(min, dv);
			max = VectorUtil.maxEqual(max, dv);

			sumWeight += weight;

			if (bCov) {
				if (dotProduction == null) {
					dotProduction = DenseMatrix.zeros(n, n);
				} else {
					if (dv.size() > dotProduction.numRows()) {
						DenseMatrix dpNew = DenseMatrix.zeros(n, n);
						dotProduction = VectorUtil.plusEqual(dpNew, dotProduction);
					}
				}

				for (int i = 0; i < dv.size(); i++) {
					double val = dv.get(i);
					for (int j = 0; j < dv.size(); j++) {
						dotProduction.add(i, j, val * dv.get(j));
					}
				}
			}
			return this;
		} else {
			SparseVectorSummarizer sparseSrt = new SparseVectorSummarizer(bCov);
			sparseSrt.visit(vec);
			sparseSrt.merge(this);
			return sparseSrt;
		}
	}

	@Override
	public BaseVectorSummarizer merge(BaseVectorSummarizer srt) {
		if (srt.count == 0) {
			return this;
		}

		if (count == 0) {
			return srt.clone();
		}

		if (srt instanceof DenseVectorSummarizer) {
			DenseVectorSummarizer vsrt = (DenseVectorSummarizer) srt;
			count += vsrt.count;
			sum = VectorUtil.plusEqual(sum, vsrt.sum);
			sum2 = VectorUtil.plusEqual(sum2, vsrt.sum2);
			normL1 = VectorUtil.plusEqual(normL1, vsrt.normL1);
			min = VectorUtil.minEqual(min, vsrt.min);
			max = VectorUtil.maxEqual(max, vsrt.max);

			sumWeight += vsrt.sumWeight;

			if (dotProduction != null && srt.dotProduction != null) {
				int size = srt.dotProduction.numRows();
				if (srt.dotProduction.numRows() > dotProduction.numRows()) {
					DenseMatrix dpNew = DenseMatrix.zeros(size, size);
					dotProduction = VectorUtil.plusEqual(dpNew, dotProduction);
				}
				dotProduction = VectorUtil.plusEqual(dotProduction, srt.dotProduction);
			} else if (dotProduction == null && srt.dotProduction != null) {
				dotProduction = srt.dotProduction.copy();
			}

			return this;
		} else {
			SparseVectorSummarizer vsrt = (SparseVectorSummarizer) srt;
			vsrt.merge(this);
			return vsrt;
		}
	}

	@Override
	public DenseVectorSummarizer clone() {
		DenseVectorSummarizer vsrt = new DenseVectorSummarizer();
		vsrt.count = count;
		vsrt.sumWeight = sumWeight;
		if (count != 0) {
			vsrt.sum = sum.clone();
			vsrt.sum2 = sum2.clone();
			vsrt.normL1 = normL1.clone();
			vsrt.min = min.clone();
			vsrt.max = max.clone();
		}

		if (dotProduction != null) {
			vsrt.dotProduction = dotProduction;
		}

		return vsrt;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder()
			.append("rowNum: " + count)
			.append("\n");
		if (count != 0) {
			sbd.append("sum: ")
				.append(sum.serialize())
				.append("\n")
				.append("sum2: ")
				.append(sum2.serialize())
				.append("\n")
				.append("min: ")
				.append(min.serialize())
				.append("\n")
				.append("max: ")
				.append(max.serialize());
		}

		return sbd.toString();
	}

	@Override
	public BaseVectorSummary toSummary() {
		DenseVectorSummary summary = new DenseVectorSummary();
		summary.count = count;
		summary.sum = sum;
		summary.sum2 = sum2;
		summary.normL1 = normL1;
		summary.min = min;
		summary.max = max;

		return summary;
	}
}
