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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.generated.RecordEqualiser;
import org.apache.flink.table.runtime.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * This function is used to get the first row or last row for every key partition in miniBatch
 * mode.
 */
public class MiniBatchDeduplicateFunction
		extends MapBundleFunction<BaseRow, BaseRow, BaseRow, BaseRow> {

	private BaseRowTypeInfo rowTypeInfo;
	private boolean generateRetraction;
	private boolean keepLastRow;
	protected ValueState<BaseRow> pkRow;
	private TypeSerializer<BaseRow> ser;
	private GeneratedRecordEqualiser generatedEqualiser;
	private transient RecordEqualiser equaliser;

	public MiniBatchDeduplicateFunction(
			BaseRowTypeInfo rowTypeInfo,
			boolean generateRetraction,
			ExecutionConfig executionConfig,
			boolean keepLastRow,
			GeneratedRecordEqualiser generatedEqualiser) {
		this.rowTypeInfo = rowTypeInfo;
		this.generateRetraction = generateRetraction;
		this.keepLastRow = keepLastRow;
		ser = rowTypeInfo.createSerializer(executionConfig);
		this.generatedEqualiser = generatedEqualiser;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ValueStateDescriptor<BaseRow> rowStateDesc = new ValueStateDescriptor("rowState", rowTypeInfo);
		pkRow = ctx.getRuntimeContext().getState(rowStateDesc);

		// compile equaliser
		equaliser = generatedEqualiser.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		generatedEqualiser = null;
	}

	@Override
	public BaseRow addInput(@Nullable BaseRow value, BaseRow input) {
		if (value == null || keepLastRow || (!keepLastRow && value == null)) {
			// put the input into buffer
			return ser.copy(input);
		} else {
			// the input is not last row, ignore it
			return value;
		}
	}

	@Override
	public void finishBundle(
			Map<BaseRow, BaseRow> buffer, Collector<BaseRow> out) throws Exception {
		for (Map.Entry<BaseRow, BaseRow> entry : buffer.entrySet()) {
			BaseRow currentKey = entry.getKey();
			BaseRow currentRow = entry.getValue();
			ctx.setCurrentKey(currentKey);
			BaseRow preRow = pkRow.value();

			if (keepLastRow) {
				DeduplicateFunctionHelper
						.processLastRow(preRow, currentRow, generateRetraction, false, pkRow, equaliser, out);
			} else {
				DeduplicateFunctionHelper
						.processFirstRow(preRow, currentRow, pkRow, out);
			}
		}
	}
}
