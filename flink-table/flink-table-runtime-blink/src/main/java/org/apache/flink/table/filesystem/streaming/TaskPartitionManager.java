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

package org.apache.flink.table.filesystem.streaming;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.table.filesystem.streaming.trigger.PartitionCommitTrigger;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.table.filesystem.PartitionPathUtils.extractPartitionSpecFromPath;

/**
 * Manage task partition and watermark.
 */
public class TaskPartitionManager {

	private static final ListStateDescriptor<Map<Long, Long>> CP_ID_WATERMARK_STATE_DESC =
			new ListStateDescriptor<>(
					"checkpoint-id-to-watermark",
					new MapSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE));
	private static final ListStateDescriptor<String> PENDING_PARTITIONS_STATE_DESC =
			new ListStateDescriptor<>("pending-partitions", StringSerializer.INSTANCE);

	private final ListState<Map<Long, Long>> cpIdToWatermarkState;
	private final ListState<String> pendingPartitions;
	private final Set<String> partitions;
	private final PartitionCommitTrigger partCommitTrigger;

	private long currentWatermark = Long.MIN_VALUE;

	public TaskPartitionManager(
			FunctionInitializationContext context,
			RuntimeContext runtimeContext,
			Map<String, String> properties) throws Exception {
		this.partCommitTrigger = PartitionCommitTrigger.createTrigger(
				properties.get(FileSystemStreamingSink.CONNECTOR_SINK_PARTITION_COMMIT_TRIGGER),
				properties.get(FileSystemStreamingSink.CONNECTOR_SINK_PARTITION_COMMIT_TRIGGER_CLASS),
				runtimeContext.getUserCodeClassLoader());

		this.cpIdToWatermarkState = context.getOperatorStateStore().getListState(CP_ID_WATERMARK_STATE_DESC);
		this.pendingPartitions = context.getOperatorStateStore().getListState(PENDING_PARTITIONS_STATE_DESC);

		Map<Long, Long> cpIdToWatermark = new HashMap<>();
		if (context.isRestored()) {
			this.cpIdToWatermarkState.get().forEach(element -> {
				element.forEach((cpId, watermark) ->
						cpIdToWatermark.compute(
								cpId,
								(k, v) -> v == null ? watermark : Math.min(watermark, v)));
			});
			this.cpIdToWatermarkState.clear();
		}
		this.cpIdToWatermarkState.add(cpIdToWatermark);
		this.partitions = new HashSet<>();
	}

	public void invokeForPartition(String partition, long currentWatermark) {
		this.currentWatermark = currentWatermark;
		if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
			partitions.add(partition);
		}
	}

	public void snapshotState(long checkpointId) throws Exception {
		updateCpIdToWatermark(checkpointId);
		updatePendingPartitions();
	}

	public List<String> triggeredPartitions(long checkpointId) throws Exception {
		long watermark = headCpIdToWatermark(checkpointId);

		List<String> parts = new ArrayList<>();
		List<String> keepPendingParts = new ArrayList<>();
		for (String part : pendingPartitions.get()) {
			if (partCommitTrigger.canCommit(
					extractPartitionSpecFromPath(new Path(part)),
					watermark)) {
				parts.add(part);
			} else {
				keepPendingParts.add(part);
			}
		}
		pendingPartitions.clear();
		pendingPartitions.addAll(keepPendingParts);
		return parts;
	}

	private long headCpIdToWatermark(long checkpointId) throws Exception {
		TreeMap<Long, Long> cpIdToWatermark = new TreeMap<>(cpIdToWatermarkState.get().iterator().next());
		cpIdToWatermarkState.clear();
		long ret = cpIdToWatermark.get(checkpointId);
		cpIdToWatermark.headMap(checkpointId, true).clear();
		cpIdToWatermarkState.add(cpIdToWatermark);
		return ret;
	}

	private void updateCpIdToWatermark(long cpId) throws Exception {
		Map<Long, Long> cpIdToWatermark = cpIdToWatermarkState.get().iterator().next();
		cpIdToWatermark.put(cpId, currentWatermark);
		cpIdToWatermarkState.clear();
		cpIdToWatermarkState.add(cpIdToWatermark);
	}

	private void updatePendingPartitions() throws Exception {
		for (String partition : pendingPartitions.get()) {
			partitions.add(partition);
		}
		pendingPartitions.clear();
		for (String partition : partitions) {
			pendingPartitions.add(partition);
		}
		partitions.clear();
	}

}
