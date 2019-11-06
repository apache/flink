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

import org.apache.flink.ml.common.linalg.Vector;

/**
 * It is the base class which is used to store vector summary result.
 * You can get vectorSize, sum, mean, variance, standardDeviation, and so on.
 *
 * <p>Summary Inheritance relationship as follow:
 *            BaseSummary
 *             /       \
 *            /         \
 *   TableSummary     BaseVectorSummary
 *                     /            \
 *                    /              \
 *      SparseVectorSummary    DenseVectorSummary
 *
 * <p>It can use toSummary() to get the result BaseVectorSummary.
 *
 * <p>example:
 * <pre>
 * {@code
 *   DenseVector data = new DenseVector(new double[]{1.0, -1.0, 3.0})
 *   DenseVectorSummarizer summarizer = new DenseVectorSummarizer(false);
 *   summarizer = summarizer.visit(data);
 *   BaseVectorSummary summary = summarizer.toSummary()
 *   double mean = summary.mean(0)
 *}
 *</pre>
 */
public abstract class BaseVectorSummary extends BaseSummary {

	/**
	 * vector size.
	 */
	public abstract int vectorSize();

	/**
	 * sum of each dimension.
	 */
	public abstract Vector sum();

	/**
	 * mean of each feature.
	 */
	public abstract Vector mean();

	/**
	 * variance of each feature.
	 */
	public abstract Vector variance();

	/**
	 * standardDeviation of each feature.
	 */
	public abstract Vector standardDeviation();

	/**
	 * min of each feature.
	 */
	public abstract Vector min();

	/**
	 * max of each feature.
	 */
	public abstract Vector max();

	/**
	 * l1 norm of each feature.
	 */
	public abstract Vector normL1();

	/**
	 * Euclidean norm of each feature.
	 */
	public abstract Vector normL2();

	/**
	 * return sum of idx feature.
	 */
	public double sum(int idx) {
		return sum().get(idx);
	}

	/**
	 * return mean of idx feature.
	 */
	public double mean(int idx) {
		return mean().get(idx);
	}

	/**
	 * return variance of idx feature.
	 */
	public double variance(int idx) {
		return variance().get(idx);
	}

	/**
	 * return standardDeviation of idx feature.
	 */
	public double standardDeviation(int idx) {
		return standardDeviation().get(idx);
	}

	/**
	 * return min of idx feature.
	 */
	public double min(int idx) {
		return min().get(idx);
	}

	/**
	 * return max of idx feature.
	 */
	public double max(int idx) {
		return max().get(idx);
	}

	/**
	 * return normL1 of idx feature.
	 */
	public double normL1(int idx) {
		return normL1().get(idx);
	}

	/**
	 * return normL2 of idx feature.
	 */
	public double normL2(int idx) {
		return normL2().get(idx);
	}

}
