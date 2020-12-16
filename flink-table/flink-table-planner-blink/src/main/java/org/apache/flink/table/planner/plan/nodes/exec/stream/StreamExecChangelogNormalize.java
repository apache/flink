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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.operators.deduplicate.ProcTimeMiniBatchDeduplicateKeepLastRowFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

/**
 * Stream exec node which normalizes a changelog stream which maybe an upsert stream or
 * a changelog stream containing duplicate events. This node normalize such stream into a regular
 * changelog stream that contains INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE records without duplication.
 */
public class StreamExecChangelogNormalize extends StreamExecNode<RowData> {
	private final int[] uniqueKeys;
	private final boolean generateUpdateBefore;

	public StreamExecChangelogNormalize(
			int[] uniqueKeys,
			boolean generateUpdateBefore,
			ExecEdge inputEdge,
			RowType outputType,
			String description) {
		super(Collections.singletonList(inputEdge), outputType, description);
		this.uniqueKeys = uniqueKeys;
		this.generateUpdateBefore = generateUpdateBefore;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Transformation<RowData> translateToPlanInternal(StreamPlanner planner) {
		final ExecNode<?> inputNode = getInputNodes().get(0);
		final Transformation<RowData> inputTransform = (Transformation<RowData>) inputNode.translateToPlan(planner);
		final InternalTypeInfo<RowData> rowTypeInfo = (InternalTypeInfo<RowData>) inputTransform.getOutputType();

		final OneInputStreamOperator<RowData, RowData> operator;
		final TableConfig tableConfig = planner.getTableConfig();
		final long stateIdleTime = tableConfig.getIdleStateRetention().toMillis();
		final boolean isMiniBatchEnabled = tableConfig.getConfiguration().getBoolean(
				ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
		if (isMiniBatchEnabled) {
			TypeSerializer<RowData> rowSerializer = rowTypeInfo.createSerializer(planner.getExecEnv().getConfig());
			ProcTimeMiniBatchDeduplicateKeepLastRowFunction processFunction = new ProcTimeMiniBatchDeduplicateKeepLastRowFunction(
					rowTypeInfo,
					rowSerializer,
					stateIdleTime,
					generateUpdateBefore,
					true, // generateInsert
					false); // inputInsertOnly
			CountBundleTrigger<RowData> trigger = AggregateUtil.createMiniBatchTrigger(tableConfig);
			operator = new KeyedMapBundleOperator<>(processFunction, trigger);
		} else {
			ProcTimeDeduplicateKeepLastRowFunction processFunction = new ProcTimeDeduplicateKeepLastRowFunction(
					rowTypeInfo,
					stateIdleTime,
					generateUpdateBefore,
					true, // generateInsert
					false); // inputInsertOnly
			operator = new KeyedProcessOperator<>(processFunction);
		}

		final OneInputTransformation<RowData, RowData> transform = new OneInputTransformation<>(
				inputTransform,
				getDesc(),
				operator,
				rowTypeInfo,
				inputTransform.getParallelism());

		if (inputsContainSingleton()) {
			transform.setParallelism(1);
			transform.setMaxParallelism(1);
		}

		final RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(uniqueKeys, rowTypeInfo);
		transform.setStateKeySelector(selector);
		transform.setStateKeyType(selector.getProducedType());

		return transform;
	}
}
