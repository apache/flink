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

package org.apache.flink.ml.pipeline.statistics;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.batchoperator.statistics.SummarizerBatchOp;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.common.statistics.StatisticsUtil;
import org.apache.flink.ml.common.statistics.basicstatistic.TableSummary;
import org.apache.flink.ml.params.statistics.SummarizerParams;
import org.apache.flink.ml.pipeline.ResultCollector;
import org.apache.flink.table.api.Table;

/**
 * Summary statistics for Table through Summarizer. User can easily get the total count and the basic column-wise
 * metrics: max, min, mean, variance, standardDeviation, normL1, normL2, the number of missing values and the number of
 * valid values.
 */
public class Summarizer extends ResultCollector <Summarizer, TableSummary>
	implements SummarizerParams <Summarizer> {

	public Summarizer(Table in) {
		super(in);
	}

	public Summarizer(Table in, Params params) {
		super(in, params);
	}

	@Override
	public TableSummary collectResult() {
		BatchOperator bin = ((BatchOperator) in);

		SummarizerBatchOp stat = new SummarizerBatchOp(params);
		stat.linkFrom(bin);

		return MLSession.jsonConverter.fromJson(
			StatisticsUtil.getJson(stat.collect()),
			TableSummary.class
		);
	}
}
