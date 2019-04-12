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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.runtime.deduplicate.DeduplicateFunctionHelper.processFirstRow;
import static org.apache.flink.table.runtime.deduplicate.DeduplicateFunctionHelper.processLastRow;

/**
 * This function is used to get the first row or last row for every key partition in miniBatch
 * mode.
 */
public class MiniBatchDeduplicateFunction
		extends MapBundleFunction<BaseRow, BaseRow, BaseRow, BaseRow> {

	private BaseRowTypeInfo rowTypeInfo;
	private boolean generateRetraction;
	private boolean keepLastRow;

	// state stores complete row if keep last row and generate retraction is true,
	// else stores a flag to indicate whether key appears before.
	private ValueState state;
	private TypeSerializer<BaseRow> ser;

	public MiniBatchDeduplicateFunction(
			BaseRowTypeInfo rowTypeInfo,
			boolean generateRetraction,
			TypeSerializer<BaseRow> typeSerializer,
			boolean keepLastRow) {
		this.rowTypeInfo = rowTypeInfo;
		this.keepLastRow = keepLastRow;
		this.generateRetraction = generateRetraction;
		ser = typeSerializer;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ValueStateDescriptor stateDesc = null;
		if (keepLastRow && generateRetraction) {
			// if need generate retraction and keep last row, stores complete row into state
			stateDesc = new ValueStateDescriptor("deduplicateFunction", rowTypeInfo);
		} else {
			// else stores a flag to indicator whether pk appears before.
			stateDesc = new ValueStateDescriptor("fistValueState", Types.BOOLEAN);
		}
		state = ctx.getRuntimeContext().getState(stateDesc);
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

			if (keepLastRow) {
				processLastRow(currentRow, generateRetraction, state, out);
			} else {
				processFirstRow(currentRow, state, out);
			}
		}
	}
}
