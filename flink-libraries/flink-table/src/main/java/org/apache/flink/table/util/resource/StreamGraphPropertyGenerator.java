/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.util.resource;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 *  StreamGraphPropertyGenerator base on rules.
 */
public class StreamGraphPropertyGenerator {

	/**
	 * Generate json format of streaming graph.
	 *
	 * @param streamGraph streamGraph
	 * @return ResourceFile
	 */
	public static StreamGraphProperty generateProperties(StreamGraph streamGraph) {
		Preconditions.checkNotNull(streamGraph, "streamGraph cannot be null");

		StreamGraphProperty properties = new StreamGraphProperty();

		Map<String, StreamNodeProperty> uidToNodeProperties = new HashMap<>();
		Map<Integer, String> nodeIdMap = new HashMap<>();

		Map<String, StreamNode> existedUidMap = StreamNodeUtil.setUid(streamGraph);

		// nodes
		for (StreamNode streamNode : streamGraph.getStreamNodes()) {
			String uid = streamNode.getTransformationUID();
			nodeIdMap.put(streamNode.getId(), uid);
			StreamNodeProperty streamNodeProperty = new StreamNodeProperty(uid);
			uidToNodeProperties.put(uid, streamNodeProperty);
			streamNodeProperty.setName(getNodeName(streamNode.getOperatorName()));
			if (streamNode.getInEdges().isEmpty()) {
				streamNodeProperty.setPact("Source");
			} else if (streamNode.getOutEdges().isEmpty()) {
				streamNodeProperty.setPact("Sink");
			} else {
				streamNodeProperty.setPact("Operator");
			}
			streamNodeProperty.setName(getNodeName(streamNode.getOperatorName()));

			// set max parallelism according to origin max parallelism
			int nodeMaxParallel = StreamNodeUtil.getMaxParallelism(streamNode);
			int maxParallelism = nodeMaxParallel > 0 ? nodeMaxParallel : StreamGraphGenerator.UPPER_BOUND_MAX_PARALLELISM;
			streamNodeProperty.setMaxParallelism(maxParallelism);

			if (streamNode.getParallelism() > 0) {
				// parallelism configured already, not changed.
				streamNodeProperty.setParallelism(streamNode.getParallelism());
			}

			ResourceSpec minResourceSpec = streamNode.getMinResources();
			Preconditions.checkArgument(minResourceSpec != null, "resource can not be null.");
			streamNodeProperty.setCpuCores(minResourceSpec.getCpuCores());
			streamNodeProperty.setHeapMemoryInMB(minResourceSpec.getHeapMemory());
			streamNodeProperty.setDirectMemoryInMB(minResourceSpec.getDirectMemory());
			streamNodeProperty.setNativeMemoryInMB(minResourceSpec.getNativeMemory());
			streamNodeProperty.setGpuLoad(minResourceSpec.getGPUResource());
			if (minResourceSpec.getExtendedResources().containsKey(ResourceSpec.MANAGED_MEMORY_NAME)) {
				streamNodeProperty.setManagedMemoryInMB(
						(int) ((minResourceSpec.getExtendedResources().get(ResourceSpec.MANAGED_MEMORY_NAME)).getValue()));
			}
			if (minResourceSpec.getExtendedResources().containsKey(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME)) {
				streamNodeProperty.setFloatingManagedMemoryInMB(
						(int) ((minResourceSpec.getExtendedResources().get(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME)).getValue()));
			}
			properties.getStreamNodeProperties().add(streamNodeProperty);
		}

		StreamNodeUtil.clearAppendUids(streamGraph, existedUidMap);

		// links
		for (StreamNode source : streamGraph.getStreamNodes()) {
			Map<Integer, Integer> edgeCounts = new HashMap<>();
			for (StreamEdge edge : source.getOutEdges()) {
				edgeCounts.putIfAbsent(edge.getTargetId(), 0);
				int index = edgeCounts.put(edge.getTargetId(), 1 + edgeCounts.get(edge.getTargetId()));
				StreamEdgeProperty streamEdgeProperty = new StreamEdgeProperty(
						nodeIdMap.get(edge.getSourceId()),
						nodeIdMap.get(edge.getTargetId()),
						index);
				streamEdgeProperty.setShipStrategy(edge.getPartitioner().toString());

				// change rescale and rebalance to forward
				StreamNodeProperty sourceNode = uidToNodeProperties.get(streamEdgeProperty.getSource());
				StreamNodeProperty targetNode = uidToNodeProperties.get(streamEdgeProperty.getTarget());

				if (sourceNode.getParallelism() == targetNode.getParallelism() &&
						sourceNode.getMaxParallelism() == targetNode.getMaxParallelism() &&
						StreamEdgeProperty.STRATEGY_UPDATABLE.contains(streamEdgeProperty.getShipStrategy())) {
					streamEdgeProperty.setShipStrategy(StreamEdgeProperty.FORWARD_STRATEGY);
				}
				properties.getStreamEdgeProperties().add(streamEdgeProperty);
			}
		}

		return properties;
	}

	private static String getNodeName(String nodeName) {
		return StringUtil.filterSpecChars(nodeName.trim())
				.replaceFirst("Source: ", "") // "Source: " will be auto added
				.replaceFirst("Sink: ", ""); // "Sink: " will be auto added
	}
}

