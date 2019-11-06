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

/**
 * Summarizer is the base class to calculate summary and store intermediate results, and Summary is the result of Summarizer.
 *
 * <p>Summarizer Inheritance relationship as follow:
 *         BaseSummarizer
 *            /       \
 *          /         \
 * TableSummarizer   BaseVectorSummarizer
 *                     /            \
 *                    /              \
 *      SparseVectorSummarizer    DenseVectorSummarizer
 *
 * <p>TableSummarizer is for table data, BaseVectorSummarizer is for vector data.
 *  SparseVectorSummarizer is for sparse vector, DenseVectorSummarizer is for dense vector.
 *
 * <p>Summary Inheritance relationship as follow:
 *            BaseSummary
 *            /       \
 *           /         \
 *  TableSummary     BaseVectorSummary
 *                     /            \
 *                    /              \
 *      SparseVectorSummary    DenseVectorSummary
 *
 * <p>You can get statistics value from summary.
 *
 * <p>example:
 * <pre>
 * {@code
 *      Row data =  Row.of("a", 1L, 1, 2.0, true)
 *      TableSummarizer summarizer = new TableSummarizer(selectedColNames, numberIdxs, bCov);
 *      summarizer = summarizer.visit(data);
 *      TableSummary summary = summarizer.toSummary()
 *      double mean = summary.mean("col")
 * }
 * </pre>
 */
public abstract class BaseSummary {

	/**
	 * count.
	 */
	protected long count;

	/**
	 * count.
	 */
	public long count() {
		return count;
	}

}
