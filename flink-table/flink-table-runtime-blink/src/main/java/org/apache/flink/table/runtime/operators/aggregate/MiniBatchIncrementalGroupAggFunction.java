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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Aggregate Function used for the incremental groupby (without window) aggregate in miniBatch mode.
 */
public class MiniBatchIncrementalGroupAggFunction extends MapBundleFunction<BaseRow, BaseRow, BaseRow, BaseRow> {

	private static final long serialVersionUID = 1L;

	/**
	 * The code generated partial function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genPartialAggsHandler;

	/**
	 * The code generated final function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genFinalAggsHandler;

	/**
	 * The key selector to extract final key.
	 */
	private final KeySelector<BaseRow, BaseRow> finalKeySelector;

	/**
	 * Reused output row.
	 */
	private transient JoinedRow resultRow = new JoinedRow();

	// local aggregate function to handle local combined accumulator rows
	private transient AggsHandleFunction partialAgg = null;

	// global aggregate function to handle global accumulator rows
	private transient AggsHandleFunction finalAgg = null;

	public MiniBatchIncrementalGroupAggFunction(
			GeneratedAggsHandleFunction genPartialAggsHandler,
			GeneratedAggsHandleFunction genFinalAggsHandler,
			KeySelector<BaseRow, BaseRow> finalKeySelector) {
		this.genPartialAggsHandler = genPartialAggsHandler;
		this.genFinalAggsHandler = genFinalAggsHandler;
		this.finalKeySelector = finalKeySelector;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ClassLoader classLoader = ctx.getRuntimeContext().getUserCodeClassLoader();
		partialAgg = genPartialAggsHandler.newInstance(classLoader);
		partialAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		finalAgg = genFinalAggsHandler.newInstance(classLoader);
		finalAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		resultRow = new JoinedRow();
	}

	@Override
	public BaseRow addInput(@Nullable BaseRow previousAcc, BaseRow input) throws Exception {
		BaseRow currentAcc;
		if (previousAcc == null) {
			currentAcc = partialAgg.createAccumulators();
		} else {
			currentAcc = previousAcc;
		}

		partialAgg.setAccumulators(currentAcc);
		partialAgg.merge(input);
		return partialAgg.getAccumulators();
	}

	@Override
	public void finishBundle(Map<BaseRow, BaseRow> buffer, Collector<BaseRow> out) throws Exception {
		// pre-aggregate for final aggregate result

		// buffer schema: [finalKey, [partialKey, partialAcc]]
		Map<BaseRow, Map<BaseRow, BaseRow>> finalAggBuffer = new HashMap<>();
		for (Map.Entry<BaseRow, BaseRow> entry : buffer.entrySet()) {
			BaseRow partialKey = entry.getKey();
			BaseRow finalKey = finalKeySelector.getKey(partialKey);
			BaseRow partialAcc = entry.getValue();
			// use compute to avoid additional put
			Map<BaseRow, BaseRow> accMap = finalAggBuffer.computeIfAbsent(finalKey, r -> new HashMap<>());
			accMap.put(partialKey, partialAcc);
		}

		for (Map.Entry<BaseRow, Map<BaseRow, BaseRow>> entry : finalAggBuffer.entrySet()) {
			BaseRow finalKey = entry.getKey();
			Map<BaseRow, BaseRow> accMap = entry.getValue();
			// set accumulators to initial value
			finalAgg.resetAccumulators();
			for (Map.Entry<BaseRow, BaseRow> accEntry : accMap.entrySet()) {
				BaseRow partialKey = accEntry.getKey();
				BaseRow partialAcc = accEntry.getValue();
				// set current key to make dataview know current key
				ctx.setCurrentKey(partialKey);
				finalAgg.merge(partialAcc);
			}
			BaseRow finalAcc = finalAgg.getAccumulators();
			resultRow.replace(finalKey, finalAcc);
			out.collect(resultRow);
		}
		// for gc friendly
		finalAggBuffer.clear();
	}

	@Override
	public void close() throws Exception {
		if (partialAgg != null) {
			partialAgg.close();
		}
		if (finalAgg != null) {
			finalAgg.close();
		}
	}
}
