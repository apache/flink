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

/**
 * It is median result of sparse vector summarizer.
 */
public class VectorStatCol {
	/**
	 * number of non-zero value.
	 */
	public long numNonZero = 0;

	/**
	 * sum of  ith dimension: sum(x_i).
	 */
	public double sum = 0;

	/**
	 * square sum of feature: sum(x_i * x_i).
	 */
	public double squareSum = 0;

	/**
	 * min of feature: min(x_i).
	 */
	public double min = Double.MAX_VALUE;

	/**
	 * max of feature: max(x_i).
	 */
	public double max = -Double.MAX_VALUE;

	/**
	 * norm l1.
	 */
	public double normL1 = 0;

	/**
	 * update by value.
	 */
	public void visit(double value) {
		if (!Double.isNaN(value)) {
			double tmp = value;
			sum += tmp;

			tmp *= value;
			squareSum += tmp;

			if (value != 0) {
				numNonZero++;
			}

			if (value < min) {
				min = value;
			}
			if (value > max) {
				max = value;
			}

			normL1 += Math.abs(value);
		}
	}

	/**
	 * merge.
	 */
	public void merge(VectorStatCol vsc) {
		numNonZero += vsc.numNonZero;
		sum += vsc.sum;
		squareSum += vsc.squareSum;
		if (vsc.min < min) {
			min = vsc.min;
		}
		if (vsc.max > max) {
			max = vsc.max;
		}
		normL1 += vsc.normL1;
	}

	@Override
	public String toString() {
		return "VectorStatCol{" +
			"numNonZero=" + numNonZero +
			", sum=" + sum +
			", squareSum=" + squareSum +
			", min=" + min +
			", max=" + max +
			", normL1=" + normL1 +
			'}';
	}

	@Override
	public VectorStatCol clone() {
		VectorStatCol vsc = new VectorStatCol();
		vsc.numNonZero = numNonZero;
		vsc.sum = sum;
		vsc.squareSum = squareSum;
		vsc.min = min;
		vsc.max = max;
		vsc.normL1 = normL1;
		return vsc;
	}

	/**
	 * mean.
	 */
	public double mean(double count) {
		if (count == 0) {
			return 0;
		} else {
			return sum / count;
		}
	}

	/**
	 * variance.
	 */
	public double variance(double count) {
		if (0 == count || 1 == count) {
			return 0;
		} else {
			return Math.max(0.0, (squareSum - mean(count) * sum) / (count - 1));
		}
	}

	/**
	 * standardDeviation.
	 */
	public double standardDeviation(double count) {
		return Math.sqrt(variance(count));
	}
}
