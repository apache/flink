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

package org.apache.flink.ml.batchoperator.statistics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.common.statistics.StatisticsUtil;
import org.apache.flink.ml.common.statistics.basicstatistic.BaseVectorSummary;
import org.apache.flink.ml.common.utils.Types;
import org.apache.flink.ml.params.Params;
import org.apache.flink.ml.params.statistics.VectorSummarizerParams;
import org.apache.flink.types.Row;

/**
 * Vector summary statistics for batch data.
 */
public class VectorSummarizerBatchOp extends BatchOperator <VectorSummarizerBatchOp>
	implements VectorSummarizerParams <VectorSummarizerBatchOp> {

	public VectorSummarizerBatchOp() {
		super(null);
	}

	public VectorSummarizerBatchOp(Params params) {
		super(params);
	}

	@Override
	public VectorSummarizerBatchOp linkFrom(BatchOperator in) {
		DataSet <BaseVectorSummary> srt = StatisticsUtil.vectorSummary(in, getSelectedColName());

		DataSet <Row> out = srt
			.map(new MapFunction <BaseVectorSummary, Row>() {
				@Override
				public Row map(BaseVectorSummary srt) throws Exception {
					Row row = new Row(1);
					row.setField(0, MLSession.gson.toJson(srt));
					return row;
				}
			});

		String[] outColNames = new String[] {"srt"};
		TypeInformation[] outColTypes = new TypeInformation[] {Types.STRING};

		this.setTable(out, outColNames, outColTypes);

		return this;
	}

}
