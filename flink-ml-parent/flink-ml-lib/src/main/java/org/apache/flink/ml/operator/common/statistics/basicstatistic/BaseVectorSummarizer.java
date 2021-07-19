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

package org.apache.flink.ml.operator.common.statistics.basicstatistic;

import org.apache.flink.ml.common.linalg.DenseMatrix;
import org.apache.flink.ml.common.linalg.Vector;

/**
 * It is the base class of vector summary. Inheritance relationship as follow:
 *             BaseSummarizer
 *               /       \
 *              /         \
 *   TableSummarizer   BaseVectorSummarizer
 *                       /            \
 *                      /              \
 *        SparseVectorSummarizer    DenseVectorSummarizer
 *
 * <p>TableSummarizer is for table data, BaseVectorSummarizer is for vector data.
 * SparseVectorSummarizer is for sparse vector, DenseVectorSummarizer is for dense vector.
 *
 * <p>It can use toSummary() to get the result BaseVectorSummary.
 *
 * <p>example:
 * <pre>
 * {@code
 *      DenseVector data = new DenseVector(new double[]{1.0, -1.0, 3.0})
 *      DenseVectorSummarizer summarizer = new DenseVectorSummarizer(false);
 *      summarizer = summarizer.visit(data);
 *      BaseVectorSummary summary = summarizer.toSummary()
 *      double mean = summary.mean(0)
 * }
 * </pre>
 */
public abstract class BaseVectorSummarizer extends BaseSummarizer {
	/**
	 * when input a vector, update result.
	 */
	public abstract BaseVectorSummarizer visit(Vector vec);

	/**
	 * return summary, you can get statistics from summary.
	 */
	public abstract BaseVectorSummary toSummary();

	/**
	 * covariance.
	 */
	@Override
	public DenseMatrix covariance() {
		if (this.outerProduct == null) {
			return null;
		}
		Vector sum = toSummary().sum();
		int nStat = sum.size();
		double[][] cov = new double[nStat][nStat];
		for (int i = 0; i < nStat; i++) {
			for (int j = i; j < nStat; j++) {
				double d = outerProduct.get(i, j);
				d = (d - sum.get(i) * sum.get(j) / count) / (count - 1);
				cov[i][j] = d;
				cov[j][i] = d;
			}
		}
		return new DenseMatrix(cov);
	}

	/**
	 * correlation.
	 */
	@Override
	public CorrelationResult correlation() {
		if (this.outerProduct == null) {
			return null;
		}
		DenseMatrix cov = covariance();
		Vector stv = toSummary().standardDeviation();
		int n = cov.numRows();
		for (int i = 0; i < n; i++) {
			for (int j = i; j < n; j++) {
				double val = cov.get(i, j);
				if (!Double.isNaN(val) && val != 0) {
					double d = val / stv.get(i) / stv.get(j);
					cov.set(i, j, d);
					cov.set(j, i, d);
				} else {
					cov.set(i, j, 0);
					cov.set(j, i, 0);
				}
			}
		}

		for (int i = 0; i < n; i++) {
			cov.set(i, i, 1.0);
		}
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (i != j) {
					if (cov.get(i, j) > 1.0) {
						cov.set(i, j, 1.0);
					} else if (cov.get(i, j) < -1.0) {
						cov.set(i, j, -1.0);
					}
				}
			}
		}

		CorrelationResult result = new CorrelationResult();
		result.correlation = cov;

		return result;
	}

}
