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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processFirstRowOnProcTime;

/**
 * This function is used to get the first row for every key partition in miniBatch mode.
 *
 * <p>The state stores a boolean flag to indicate whether key appears before as an optimization.
 */
public class ProcTimeMiniBatchDeduplicateKeepFirstRowFunction
		extends MiniBatchDeduplicateFunctionBase<Boolean, RowData, RowData, RowData, RowData> {

	private static final long serialVersionUID = -7994602893547654994L;
	private final TypeSerializer<RowData> serializer;

	public ProcTimeMiniBatchDeduplicateKeepFirstRowFunction(
			TypeSerializer<RowData> serializer,
			long stateRetentionTime) {
		super(Types.BOOLEAN, stateRetentionTime);
		this.serializer = serializer;
	}

	@Override
	public RowData addInput(@Nullable RowData value, RowData input) {
		if (value == null) {
			// put the input into buffer
			return serializer.copy(input);
		} else {
			// the input is not first row, ignore it
			return value;
		}
	}

	@Override
	public void finishBundle(
			Map<RowData, RowData> buffer, Collector<RowData> out) throws Exception {
		for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
			RowData currentKey = entry.getKey();
			RowData currentRow = entry.getValue();
			ctx.setCurrentKey(currentKey);
			processFirstRowOnProcTime(currentRow, state, out);
		}
	}
}
