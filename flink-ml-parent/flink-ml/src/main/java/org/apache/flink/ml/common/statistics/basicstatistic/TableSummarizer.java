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
import org.apache.flink.types.Row;

/**
 * It is summarizer of table， it can handle multiple data types.
 * It uses DenseVector to store median result.
 */
public class TableSummarizer extends BaseSummarizer {

	//col names which can compute.
	protected String[] colNames;
	protected int[] numberIdxs;

	protected DenseVector numMissingValue;
	protected DenseVector sum;
	protected DenseVector sum2;
	protected DenseVector min;
	protected DenseVector max;
	protected DenseVector normL1;

	private Double[] vals;

	public TableSummarizer() {
		this.bCov = false;
	}

	public TableSummarizer(boolean bCov, int[] numberIdxs) {
		this.bCov = bCov;
		this.numberIdxs = numberIdxs;
	}

	public static TableSummarizer merge(TableSummarizer left, TableSummarizer right) {
		try {
			TableSummarizer result = left.clone();
			result = result.merge(right);
			return result;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public BaseSummarizer visit(Row row) {
		int n = row.getArity();
		int numberN = numberIdxs.length;

		if (count == 0) {
			init(n);
		}

		count++;

		for (int i = 0; i < n; i++) {
			Object obj = row.getField(i);
			if (obj == null) {
				numMissingValue.add(i, 1);
			}
		}

		for (int i = 0; i < numberN; i++) {
			Object obj = row.getField(numberIdxs[i]);
			if (obj != null) {
				vals[i] = ((Number) obj).doubleValue();
			} else {
				vals[i] = null;
			}
		}
		for (int i = 0; i < numberIdxs.length; i++) {
			if (vals[i] != null) {
				double val = vals[i];

				max.set(i, Math.max(val, max.get(i)));
				min.set(i, Math.min(val, min.get(i)));

				sum.add(i, val);
				sum2.add(i, val * val);

				normL1.add(i, Math.abs(val));

				if (bCov) {
					for (int j = 0; j < numberIdxs.length; j++) {
						if (vals[j] != null) {
							dotProduction.add(i, j, val * vals[j]);
						}
					}
				}
			}
		}
		return this;
	}

	private void init(int n) {
		int numberN = numberIdxs.length;

		numMissingValue = new DenseVector(n);
		if (numberN > 0) {
			sum = new DenseVector(numberN);
			sum2 = new DenseVector(numberN);
			normL1 = new DenseVector(numberN);
			double[] minVals = new double[numberN];
			for (int i = 0; i < numberN; i++) {
				minVals[i] = Double.MAX_VALUE;
			}
			min = new DenseVector(minVals);

			double[] maxVals = new double[numberN];
			for (int i = 0; i < numberN; i++) {
				maxVals[i] = Double.MIN_VALUE;
			}
			max = new DenseVector(maxVals);

			if (bCov) {
				dotProduction = new DenseMatrix(numberN, numberN);
			}

			vals = new Double[numberN];
		}
	}

	public TableSummarizer merge(TableSummarizer srt) {
		if (srt.count == 0) {
			return this;
		}

		if (count == 0) {
			return srt.clone();
		}

		if (srt instanceof TableSummarizer) {
			TableSummarizer vsrt = srt;
			count += vsrt.count;
			numMissingValue = VectorUtil.plusEqual(numMissingValue, vsrt.numMissingValue);
			sum = VectorUtil.plusEqual(sum, vsrt.sum);
			sum2 = VectorUtil.plusEqual(sum2, vsrt.sum2);
			normL1 = VectorUtil.plusEqual(normL1, vsrt.normL1);
			min = VectorUtil.minEqual(min, vsrt.min);
			max = VectorUtil.maxEqual(max, vsrt.max);

			if (dotProduction != null && srt.dotProduction != null) {
				dotProduction = VectorUtil.plusEqual(dotProduction, srt.dotProduction);
			} else if (dotProduction == null && srt.dotProduction != null) {
				dotProduction = srt.dotProduction.copy();
			}

			return this;
		} else {
			throw new RuntimeException("It must be TableSummarizer class.");
		}
	}

	@Override
	public TableSummarizer clone() {
		TableSummarizer vsrt = new TableSummarizer();
		vsrt.colNames = colNames;
		vsrt.count = count;
		vsrt.numberIdxs = numberIdxs;
		if (count != 0) {
			vsrt.numMissingValue = numMissingValue.clone();
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
			.append("count: " + count)
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

	public TableSummary toSummary() {
		TableSummary summary = new TableSummary();

		summary.count = count;
		summary.sum = sum;
		summary.sum2 = sum2;
		summary.normL1 = normL1;
		summary.min = min;
		summary.max = max;

		summary.numMissingValue = numMissingValue;
		summary.numberIdxs = numberIdxs;

		summary.colNames = colNames;

		return summary;
	}

	@Override
	public CorrelationResult correlation() {
		if (dotProduction != null) {
			DenseMatrix cov = covariance();
			TableSummary summary = toSummary();
			double[] stv = new double[summary.colNum()];
			for (int i = 0; i < stv.length; i++) {
				stv[i] = summary.standardDeviation(summary.colNames[i]);
			}

			int n = cov.numRows();
			for (int i = 0; i < n; i++) {
				for (int j = i; j < n; j++) {
					double val = cov.get(i, j);
					if (!Double.isNaN(val)) {
						if (val != 0) {
							double d = val / stv[i] / stv[j];
							cov.set(i, j, d);
							cov.set(j, i, d);
						}
					}
				}
			}

			for (int i = 0; i < n; i++) {
				if (!Double.isNaN(cov.get(i, i))) {
					cov.set(i, i, 1.0);
				}
			}
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < n; j++) {
					if (i != j) {
						if (!Double.isNaN(cov.get(i, j))) {
							if (cov.get(i, j) > 1.0) {
								cov.set(i, j, 1.0);
							} else if (cov.get(i, j) < -1.0) {
								cov.set(i, j, -1.0);
							}
						}
					}
				}
			}

			CorrelationResult result = new CorrelationResult();
			result.colNames = colNames;
			result.corr = cov;

			return result;
		} else {
			return null;
		}
	}

	@Override
	public DenseMatrix covariance() {
		if (null != this.dotProduction) {
			int nStat = numMissingValue.size();
			TableSummary summary = toSummary();

			double[][] cov = new double[nStat][nStat];
			for (int i = 0; i < nStat; i++) {
				for (int j = 0; j < nStat; j++) {
					cov[i][j] = Double.NaN;
				}
			}
			for (int i = 0; i < numberIdxs.length; i++) {
				int idxI = numberIdxs[i];
				double cnt = summary.numValidValue(colNames[idxI]);
				for (int j = i; j < numberIdxs.length; j++) {
					int idxJ = numberIdxs[j];
					double d = dotProduction.get(i, j);
					d = (d - summary.sum(colNames[idxI]) * summary.sum(colNames[idxJ]) / cnt) / (cnt - 1);
					cov[idxI][idxJ] = d;
					cov[idxJ][idxI] = d;
				}
			}
			return new DenseMatrix(cov);
		} else {
			return null;
		}

	}

}
