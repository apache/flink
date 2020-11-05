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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;

import static org.apache.flink.table.runtime.util.StateTtlConfigUtil.createTtlConfig;

/**
 * Base class for miniBatch deduplicate function.
 * @param <T>   The type of the value in the state.
 * @param <K>   The type of the key in the bundle map.
 * @param <V>   The type of the value in the bundle map.
 * @param <IN>  Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
abstract class MiniBatchDeduplicateFunctionBase<T, K, V, IN, OUT> extends MapBundleFunction<K, V, IN, OUT> {

	private static final long serialVersionUID = 1L;
	protected final TypeInformation<T> stateType;
	protected final long minRetentionTime;
	// state stores previous message under the key.
	protected ValueState<T> state;

	public MiniBatchDeduplicateFunctionBase(
			TypeInformation<T> stateType,
			long minRetentionTime) {
		this.stateType = stateType;
		this.minRetentionTime = minRetentionTime;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		ValueStateDescriptor<T> stateDesc = new ValueStateDescriptor<>("deduplicate-state", stateType);
		StateTtlConfig ttlConfig = createTtlConfig(minRetentionTime);
		if (ttlConfig.isEnabled()) {
			stateDesc.enableTimeToLive(ttlConfig);
		}
		state = ctx.getRuntimeContext().getState(stateDesc);
	}
}
