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

import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.HashMap;
import java.util.Map;

/**
 * StreamGraph Configurer which applies {@link StreamGraphProperty} to {@link StreamGraph}.
 */
public class StreamGraphConfigurer {

	public static void configure(StreamGraph graph, StreamGraphProperty property) {
		if (graph == null || property == null) {
			return;
		}
		Map<String, StreamNode> existedUidMap = StreamNodeUtil.setUid(graph);

		Map<String, Integer> transformationIdMap = new HashMap<>();

		int edgesInStreamGraph = 0;
		for (StreamNode node : graph.getStreamNodes()) {
			edgesInStreamGraph += node.getInEdges().size();
			transformationIdMap.put(node.getTransformationUID(), node.getId());
		}
		if (edgesInStreamGraph != property.getStreamEdgeProperties().size() ||
				graph.getStreamNodes().size() != property.getStreamNodeProperties().size()) {
			throw new RuntimeException("Node or edge number in stream graph and json file not matched, please generate a new json.");
		}

		for (StreamNodeProperty nodeProperty : property.getStreamNodeProperties()) {
			Integer nodeId = transformationIdMap.get(nodeProperty.getUid());
			if (nodeId == null) {
				throw new RuntimeException("Fail to apply resource configuration file, node " + nodeProperty.toString() + " not found.");
			}
			StreamNode node = graph.getStreamNode(nodeId);
			if (node == null) {
				throw new RuntimeException("Fail to apply resource configuration file, node " + nodeProperty.toString() + " not found.");
			}
			nodeProperty.apple(node);
		}

		for (StreamEdgeProperty edgeProperty : property.getStreamEdgeProperties()) {
			try {
				StreamEdge edge = graph.getStreamEdges(
						transformationIdMap.get(edgeProperty.getSource()),
						transformationIdMap.get(edgeProperty.getTarget())).get(edgeProperty.getIndex());
				if (edge == null) {
					throw new RuntimeException("Fail to apply resource configuration file, edge " + edgeProperty.toString() + " not found.");
				}
				edgeProperty.apply(edge, graph);
			} catch (Exception ex) {
				throw new RuntimeException("Fail to apply resource configuration file, edge " + edgeProperty.toString() + " not found.");
			}
		}
		StreamNodeUtil.clearAppendUids(graph, existedUidMap);
	}

}

