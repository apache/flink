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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processLastRow;

/**
 * This function is used to get the last row for every key partition in miniBatch mode.
 */
public class MiniBatchDeduplicateKeepLastRowFunction
		extends MapBundleFunction<BaseRow, BaseRow, BaseRow, BaseRow> {

	private static final long serialVersionUID = -8981813609115029119L;

	private final BaseRowTypeInfo rowTypeInfo;
	private final boolean generateRetraction;
	private final TypeSerializer<BaseRow> typeSerializer;

	// state stores complete row.
	private ValueState<BaseRow> state;

	public MiniBatchDeduplicateKeepLastRowFunction(BaseRowTypeInfo rowTypeInfo, boolean generateRetraction,
			TypeSerializer<BaseRow> typeSerializer) {
		this.rowTypeInfo = rowTypeInfo;
		this.generateRetraction = generateRetraction;
		this.typeSerializer = typeSerializer;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ValueStateDescriptor<BaseRow> stateDesc = new ValueStateDescriptor<>("preRowState", rowTypeInfo);
		state = ctx.getRuntimeContext().getState(stateDesc);
	}

	@Override
	public BaseRow addInput(@Nullable BaseRow value, BaseRow input) {
		// always put the input into buffer
		return typeSerializer.copy(input);
	}

	@Override
	public void finishBundle(
			Map<BaseRow, BaseRow> buffer, Collector<BaseRow> out) throws Exception {
		for (Map.Entry<BaseRow, BaseRow> entry : buffer.entrySet()) {
			BaseRow currentKey = entry.getKey();
			BaseRow currentRow = entry.getValue();
			ctx.setCurrentKey(currentKey);
			processLastRow(currentRow, generateRetraction, state, out);
		}
	}
}
