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

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * It is table summarizer partition of one worker, will merge result later.
 */
public class TableSummarizerPartiiton implements MapPartitionFunction <Row, TableSummarizer> {
	private boolean bCov;
	private int[] numberIdxs;
	private String[] selectedColNames;

	public TableSummarizerPartiiton(boolean bCov, int[] numberIdxs, String[] selectedColNames) {
		this.bCov = bCov;
		this.numberIdxs = numberIdxs;
		this.selectedColNames = selectedColNames;
	}

	@Override
	public void mapPartition(Iterable <Row> iterable, Collector <TableSummarizer> collector) throws Exception {
		TableSummarizer vsrt = new TableSummarizer(bCov, numberIdxs);
		vsrt.colNames = selectedColNames;
		for (Row sv : iterable) {
			vsrt = (TableSummarizer) vsrt.visit(sv);
		}

		collector.collect(vsrt);
	}
}

