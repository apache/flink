/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processLastRow;
import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * This function is used to deduplicate on keys and keeps only last row.
 */
public class DeduplicateKeepLastRowFunction
		extends KeyedProcessFunction<RowData, RowData, RowData> {

	private static final long serialVersionUID = -291348892087180350L;
	private final RowDataTypeInfo rowTypeInfo;
	private final boolean generateUpdateBefore;
	private final boolean generateInsert;

	private final long minRetentionTime;
	// state stores complete row.
	private ValueState<RowData> state;

	public DeduplicateKeepLastRowFunction(
			long minRetentionTime,
			RowDataTypeInfo rowTypeInfo,
			boolean generateUpdateBefore,
			boolean generateInsert) {
		this.minRetentionTime = minRetentionTime;
		this.rowTypeInfo = rowTypeInfo;
		this.generateUpdateBefore = generateUpdateBefore;
		this.generateInsert = generateInsert;
	}

	@Override
	public void open(Configuration configure) throws Exception {
		super.open(configure);
		ValueStateDescriptor<RowData> stateDesc = new ValueStateDescriptor<>("preRowState", rowTypeInfo);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			stateDesc.enableTimeToLive(ttlConfig);
		}
		state = getRuntimeContext().getState(stateDesc);
	}

	@Override
	public void processElement(RowData input, Context ctx, Collector<RowData> out) throws Exception {
		processLastRow(input, generateUpdateBefore, generateInsert, state, out);
	}

}
