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
import org.apache.flink.ml.common.matrix.Vector;
import org.apache.flink.ml.common.statistics.StatisticsUtil;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * It is result of DenseVectorSummarizer.
 * You can get vectorSize, mean, variance, and other statistics from this class.
 */
public class DenseVectorSummary extends BaseVectorSummary {

	protected DenseVector sum;
	protected DenseVector sum2;
	protected DenseVector sum3;
	protected DenseVector sum4;

	protected DenseVector min;
	protected DenseVector max;
	protected DenseVector normL1;

	public DenseVectorSummary() {

	}

	@Override
	public String toString() {
		String[] outColNames = new String[] {"id", "count",
			"sum", "mean", "variance", "standardDeviation", "min", "max", "normL1", "normL2"};

		List <Row> data = new ArrayList();

		for (int i = 0; i < vectorSize(); i++) {
			Row row = new Row(outColNames.length);

			row.setField(0, i);
			row.setField(1, count);
			row.setField(2, sum(i));
			row.setField(3, mean(i));
			row.setField(4, variance(i));
			row.setField(5, standardDeviation(i));
			row.setField(6, min(i));
			row.setField(7, max(i));
			row.setField(8, normL1(i));
			row.setField(9, normL2(i));

			data.add(row);
		}

		return StatisticsUtil.toString(outColNames, data);
	}

	@Override
	public int vectorSize() {
		return sum.size();
	}

	@Override
	public Vector sum() {
		return sum;
	}

	@Override
	public Vector mean() {
		if (count == 0) {
			return sum;
		} else {
			return sum.scale(1.0 / count);
		}
	}

	@Override
	public Vector variance() {
		if (0 == count || 1 == count) {
			return DenseVector.zeros(sum.size());
		} else {
			DenseVector dv = (DenseVector) mean();
			double[] means = dv.getData();
			for (int i = 0; i < means.length; i++) {
				means[i] = Math.max(0.0, (sum2.get(i) - means[i] * sum.get(i)) / (count - 1));
			}
			return dv;
		}
	}

	@Override
	public Vector standardDeviation() {
		DenseVector dv = (DenseVector) variance();
		double[] vars = dv.getData();
		for (int i = 0; i < vars.length; i++) {
			vars[i] = Math.sqrt(vars[i]);
		}
		return dv;
	}

	@Override
	public Vector min() {
		return min;
	}

	@Override
	public Vector max() {
		return max;
	}

	@Override
	public Vector normL1() {
		return normL1;
	}

	@Override
	public Vector normL2() {
		DenseVector normL2 = sum2.clone();
		DenseVector.apply(normL2, normL2, (a -> Math.sqrt(a)));
		return normL2;
	}

	@Override
	public double sum(int idx) {
		return sum.get(idx);
	}

	@Override
	public double mean(int idx) {
		return mean().get(idx);
	}

	@Override
	public double variance(int idx) {
		return variance().get(idx);
	}

	@Override
	public double standardDeviation(int idx) {
		return standardDeviation().get(idx);
	}

	@Override
	public double min(int idx) {
		return min().get(idx);
	}

	@Override
	public double max(int idx) {
		return max().get(idx);
	}

	@Override
	public double normL1(int idx) {
		return normL1().get(idx);
	}

	@Override
	public double normL2(int idx) {
		return normL2().get(idx);
	}

	@Override
	public DenseVectorSummary clone() {
		DenseVectorSummary vsrt = new DenseVectorSummary();

		vsrt.count = count;
		if (count != 0) {
			vsrt.sum = sum.clone();
			vsrt.sum2 = sum2.clone();
			vsrt.sum3 = sum3.clone();
			vsrt.sum4 = sum4.clone();
			vsrt.normL1 = normL1.clone();
			vsrt.min = min.clone();
			vsrt.max = max.clone();
		}

		return vsrt;
	}
}
