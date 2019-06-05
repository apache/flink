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
import org.apache.flink.ml.common.matrix.Vector;

/**
 * It is the base class of vector summarizer, which will calculate count, sum, sum2, min, max.
 * If sparse vector， it will calculate numNonZero of sparse vector.
 */
public abstract class BaseVectorSummarizer extends BaseSummarizer {

	protected double sumWeight = 0;

	public static BaseVectorSummarizer merge(BaseVectorSummarizer left, BaseVectorSummarizer right) {
		try {
			BaseVectorSummarizer result = left.clone();
			result = result.merge(right);
			return result;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public abstract BaseVectorSummarizer visit(Vector vec);

	public BaseVectorSummarizer visit(Vector vec, double weight) {
		throw new RuntimeException("It is support yet.");
	}

	public abstract BaseVectorSummarizer merge(BaseVectorSummarizer vsrt);

	public abstract BaseVectorSummary toSummary();

	@Override
	public abstract BaseVectorSummarizer clone();

	@Override
	public DenseMatrix covariance() {
		if (null != this.dotProduction) {
			Vector sum = toSummary().sum();
			int nStat = sum.size();
			double[][] cov = new double[nStat][nStat];
			for (int i = 0; i < nStat; i++) {
				for (int j = i; j < nStat; j++) {
					double d = dotProduction.get(i, j);
					long cnt = count;
					d = (d - sum.get(i) * sum.get(j) / cnt) / (cnt - 1);
					cov[i][j] = d;
					cov[j][i] = d;
				}
			}
			return new DenseMatrix(cov);
		} else {
			return null;
		}

	}

	@Override
	public CorrelationResult correlation() {
		if (dotProduction != null) {
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
			result.corr = cov;

			return result;

		} else {
			return null;
		}
	}

}
