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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * Aggregate Function used for the incremental groupby (without window) aggregate in miniBatch mode.
 */
public class MiniBatchIncrementalGroupAggFunction extends MapBundleFunction<RowData, RowData, RowData, RowData> {

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
	private final KeySelector<RowData, RowData> finalKeySelector;

	/**
	 * State idle retention time which unit is MILLISECONDS.
	 */
	private final long stateRetentionTime;

	/**
	 * Reused output row.
	 */
	private transient JoinedRowData resultRow = new JoinedRowData();

	// local aggregate function to handle local combined accumulator rows
	private transient AggsHandleFunction partialAgg = null;

	// global aggregate function to handle global accumulator rows
	private transient AggsHandleFunction finalAgg = null;

	public MiniBatchIncrementalGroupAggFunction(
			GeneratedAggsHandleFunction genPartialAggsHandler,
			GeneratedAggsHandleFunction genFinalAggsHandler,
			KeySelector<RowData, RowData> finalKeySelector,
			long stateRetentionTime) {
		this.genPartialAggsHandler = genPartialAggsHandler;
		this.genFinalAggsHandler = genFinalAggsHandler;
		this.finalKeySelector = finalKeySelector;
		this.stateRetentionTime = stateRetentionTime;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ClassLoader classLoader = ctx.getRuntimeContext().getUserCodeClassLoader();
		StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
		partialAgg = genPartialAggsHandler.newInstance(classLoader);
		partialAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		finalAgg = genFinalAggsHandler.newInstance(classLoader);
		finalAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext(), ttlConfig));

		resultRow = new JoinedRowData();
	}

	@Override
	public RowData addInput(@Nullable RowData previousAcc, RowData input) throws Exception {
		RowData currentAcc;
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
	public void finishBundle(Map<RowData, RowData> buffer, Collector<RowData> out) throws Exception {
		// pre-aggregate for final aggregate result

		// buffer schema: [finalKey, [partialKey, partialAcc]]
		Map<RowData, Map<RowData, RowData>> finalAggBuffer = new HashMap<>();
		for (Map.Entry<RowData, RowData> entry : buffer.entrySet()) {
			RowData partialKey = entry.getKey();
			RowData finalKey = finalKeySelector.getKey(partialKey);
			RowData partialAcc = entry.getValue();
			// use compute to avoid additional put
			Map<RowData, RowData> accMap = finalAggBuffer.computeIfAbsent(finalKey, r -> new HashMap<>());
			accMap.put(partialKey, partialAcc);
		}

		for (Map.Entry<RowData, Map<RowData, RowData>> entry : finalAggBuffer.entrySet()) {
			RowData finalKey = entry.getKey();
			Map<RowData, RowData> accMap = entry.getValue();
			// set accumulators to initial value
			finalAgg.resetAccumulators();
			for (Map.Entry<RowData, RowData> accEntry : accMap.entrySet()) {
				RowData partialKey = accEntry.getKey();
				RowData partialAcc = accEntry.getValue();
				// set current key to make dataview know current key
				ctx.setCurrentKey(partialKey);
				finalAgg.merge(partialAcc);
			}
			RowData finalAcc = finalAgg.getAccumulators();
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
