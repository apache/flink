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

import org.apache.flink.ml.common.matrix.DenseVector;
import org.apache.flink.ml.common.statistics.StatisticsUtil;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * It is summarizer result of sparse vector.
 * You can get vectorSize, mean, variance, and other statistics from this class,
 * and get statistics with colName.
 */
public class TableSummary extends BaseSummary {

	//col names which can compute.
	protected String[] colNames;

	protected DenseVector numMissingValue;

	protected DenseVector sum;
	protected DenseVector sum2;
	protected DenseVector min;
	protected DenseVector max;
	protected DenseVector normL1;

	protected int[] numberIdxs;

	public TableSummary() {

	}

	private static int findIdx(int[] numberIdxs, int idx) {
		for (int i = 0; i < numberIdxs.length; i++) {
			if (idx == numberIdxs[i]) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public TableSummary clone() {
		TableSummary vsrt = new TableSummary();

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

		return vsrt;
	}

	@Override
	public String toString() {
		String[] outColNames = new String[] {"colName", "count", "numMissingValue", "numValidValue",
			"sum", "mean", "variance", "standardDeviation", "min", "max", "normL1", "normL2"};

		List <Row> data = new ArrayList();

		for (int i = 0; i < colNum(); i++) {
			Row row = new Row(outColNames.length);

			String colName = colNames[i];
			row.setField(0, colName);
			row.setField(1, count);
			row.setField(2, numMissingValue(colName));
			row.setField(3, numValidValue(colName));
			row.setField(4, sum(colName));
			row.setField(5, mean(colName));
			row.setField(6, variance(colName));
			row.setField(7, standardDeviation(colName));
			row.setField(8, min(colName));
			row.setField(9, max(colName));
			row.setField(10, normL1(colName));
			row.setField(11, normL2(colName));

			data.add(row);
		}

		return StatisticsUtil.toString(outColNames, data);
	}

	public int colNum() {
		return this.numMissingValue.size();
	}

	public String[] getColNames() {
		return this.colNames.clone();
	}

	public double sum(String colName) {
		int idx = findIdx(colName);
		if (idx >= 0) {
			return sum.get(idx);
		} else {
			return Double.NaN;
		}
	}

	public double mean(String colName) {
		int idx = findIdx(colName);
		if (idx >= 0) {
			double numVaildValue = count - numMissingValue.get(idx);
			if (0 == numVaildValue) {
				return 0;
			}
			return sum.get(idx) / numVaildValue;
		} else {
			return Double.NaN;
		}
	}

	public double variance(String colName) {
		int idx = findIdx(colName);
		if (idx >= 0) {
			double numVaildValue = count - numMissingValue.get(idx);
			if (0 == numVaildValue || 1 == numVaildValue) {
				return 0;
			}
			return Math.max(0.0, (sum2.get(idx) - sum.get(idx) * sum.get(idx) / numVaildValue) / (numVaildValue - 1));
		} else {
			return Double.NaN;
		}
	}

	public double standardDeviation(String colName) {
		return Math.sqrt(variance(colName));
	}

	public double min(String colName) {
		int idx = findIdx(colName);
		if (idx >= 0) {
			return min.get(idx);
		} else {
			return Double.NaN;
		}
	}

	public double max(String colName) {
		int idx = findIdx(colName);
		if (idx >= 0) {
			return max.get(idx);
		} else {
			return Double.NaN;
		}
	}

	public double normL1(String colName) {
		int idx = findIdx(colName);
		if (idx >= 0) {
			return normL1.get(idx);
		} else {
			return Double.NaN;
		}
	}

	public double normL2(String colName) {
		int idx = findIdx(colName);
		if (idx >= 0) {
			return Math.sqrt(sum2.get(idx));
		} else {
			return Double.NaN;
		}
	}

	public double numValidValue(String colName) {
		return count - numMissingValue(colName);
	}

	public double numMissingValue(String colName) {
		int idx = TableUtil.findIndexFromName(colNames, colName);
		if (idx < 0) {
			throw new RuntimeException(colName + " is not exist.");
		}
		return numMissingValue.get(idx);
	}

	private int findIdx(String colName) {
		int idx = TableUtil.findIndexFromName(colNames, colName);
		if (idx < 0) {
			throw new RuntimeException(colName + " is not exist.");
		}
		return findIdx(numberIdxs, idx);
	}
}
