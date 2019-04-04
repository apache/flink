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

package org.apache.flink.table.runtime.deduplicate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.generated.RecordEqualiser;
import org.apache.flink.table.runtime.functions.KeyedProcessFunctionWithCleanupState;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

/**
 * This function is used to deduplicate on keys and keeps only first row or last row.
 */
public class DeduplicateFunction
		extends KeyedProcessFunctionWithCleanupState<BaseRow, BaseRow, BaseRow>
		implements DeduplicateFunctionBase {

	private final BaseRowTypeInfo rowTypeInfo;
	private final boolean generateRetraction;
	private final boolean keepLastRow;
	protected ValueState<BaseRow> pkRow;
	private GeneratedRecordEqualiser generatedEqualiser;
	private transient RecordEqualiser equaliser;

	public DeduplicateFunction(
			long minRetentionTime,
			long maxRetentionTime,
			BaseRowTypeInfo rowTypeInfo,
			boolean generateRetraction,
			boolean keepLastRow,
			GeneratedRecordEqualiser generatedEqualiser) {
		super(minRetentionTime, maxRetentionTime);
		this.rowTypeInfo = rowTypeInfo;
		this.generateRetraction = generateRetraction;
		this.keepLastRow = keepLastRow;
		this.generatedEqualiser = generatedEqualiser;
	}

	@Override
	public void open(Configuration configure) throws Exception {
		super.open(configure);
		String stateName = keepLastRow ? "DeduplicateFunctionCleanupTime" : "DeduplicateFunctionCleanupTime";
		initCleanupTimeState(stateName);
		ValueStateDescriptor rowStateDesc = new ValueStateDescriptor("rowState", rowTypeInfo);
		pkRow = getRuntimeContext().getState(rowStateDesc);
		equaliser = generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
	}

	@Override
	public void processElement(BaseRow input, Context ctx, Collector<BaseRow> out) throws Exception {
		long currentTime = ctx.timerService().currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(ctx, currentTime);

		BaseRow preRow = pkRow.value();
		if (keepLastRow) {
			processLastRow(preRow, input, generateRetraction, stateCleaningEnabled, pkRow, equaliser, out);
		} else {
			processFirstRow(preRow, input, generateRetraction, stateCleaningEnabled, pkRow, equaliser, out);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public void onTimer(
			long timestamp,
			OnTimerContext ctx,
			Collector<BaseRow> out) throws Exception {
		if (needToCleanupState(timestamp)) {
			cleanupState(pkRow);
		}
	}
}
