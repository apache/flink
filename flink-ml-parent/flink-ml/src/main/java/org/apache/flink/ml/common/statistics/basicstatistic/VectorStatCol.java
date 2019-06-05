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
	public long numNonZero = 0;
	public double sum = 0;
	public double sum2 = 0;
	public double min = Double.MAX_VALUE;
	public double max = Double.MIN_VALUE;
	public double normL1 = 0;

	public void visit(double value) {
		double tmp = value;
		sum += tmp;

		tmp *= value;
		sum2 += tmp;

		numNonZero++;

		if (value < min) {
			min = value;
		} else {
			if (value > max) {
				max = value;
			}
		}

		normL1 += Math.abs(value);
	}

	public void visit(double value, double weight) {
		double tmp = value;
		sum += tmp * weight;

		tmp *= value;
		sum2 += tmp * weight;

		numNonZero++;

		if (value < min) {
			min = value;
		} else {
			if (value > max) {
				max = value;
			}
		}

		normL1 += Math.abs(value) * weight;
	}

	public void merge(VectorStatCol vsc) {
		numNonZero += vsc.numNonZero;
		sum += vsc.sum;
		sum2 += vsc.sum2;
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
			", sum2=" + sum2 +
			", min=" + min +
			", max=" + max +
			", normL1=" + normL1 +
			'}';
	}

	@Override
	public VectorStatCol clone() throws CloneNotSupportedException {
		VectorStatCol vsc = new VectorStatCol();
		vsc.numNonZero = numNonZero;
		vsc.sum = sum;
		vsc.sum2 = sum2;
		vsc.min = min;
		vsc.max = max;
		vsc.normL1 = normL1;
		return vsc;
	}

	public double mean(double count) {
		if (count == 0) {
			return 0;
		} else {
			return sum / count;
		}
	}

	public double variance(double count) {
		if (0 == count || 1 == count) {
			return 0;
		} else {
			return Math.max(0.0, (sum2 - mean(count) * sum) / (count - 1));
		}
	}

	public double standardDeviation(double count) {
		return Math.sqrt(variance(count));
	}
}
