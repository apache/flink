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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.dataformat.util.BaseRowUtil.isAccumulateMsg;

/**
 * Aggregate Function used for the local groupby (without window) aggregate in miniBatch mode.
 */
public class MiniBatchLocalGroupAggFunction extends MapBundleFunction<BaseRow, BaseRow, BaseRow, BaseRow> {

	private static final long serialVersionUID = 5417039295967495506L;

	/**
	 * The code generated function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genAggsHandler;

	/**
	 * Reused output row.
	 */
	private transient JoinedRow resultRow = new JoinedRow();

	// function used to handle all aggregates
	private transient AggsHandleFunction function = null;

	public MiniBatchLocalGroupAggFunction(GeneratedAggsHandleFunction genAggsHandler) {
		this.genAggsHandler = genAggsHandler;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		// instantiate function
		function = genAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		function.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		resultRow = new JoinedRow();
	}

	@Override
	public BaseRow addInput(@Nullable BaseRow previousAcc, BaseRow input) throws Exception {
		BaseRow currentAcc;
		if (previousAcc == null) {
			currentAcc = function.createAccumulators();
		} else {
			currentAcc = previousAcc;
		}
		function.setAccumulators(currentAcc);
		if (isAccumulateMsg(input)) {
			function.accumulate(input);
		} else {
			function.retract(input);
		}
		// return the updated accumulators
		return function.getAccumulators();
	}

	@Override
	public void finishBundle(Map<BaseRow, BaseRow> buffer, Collector<BaseRow> out) throws Exception {
		for (Map.Entry<BaseRow, BaseRow> entry : buffer.entrySet()) {
			BaseRow currentKey = entry.getKey();
			BaseRow currentAcc = entry.getValue();
			resultRow.replace(currentKey, currentAcc);
			out.collect(resultRow);
		}
		buffer.clear();
	}

	@Override
	public void close() throws Exception {
		if (function != null) {
			function.close();
		}
	}
}
