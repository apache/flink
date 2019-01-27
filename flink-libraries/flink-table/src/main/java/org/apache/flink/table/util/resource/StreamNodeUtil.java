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

import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Util to access Steam Node.
 */
public class StreamNodeUtil {
	private static final Logger LOG = LoggerFactory.getLogger(StreamNodeUtil.class);

	/**
	 * Set uid for streamNode in the streamGraph.
	 * @param streamGraph SteamGraph.
	 */
	public static Map<String, StreamNode> setUid(StreamGraph streamGraph) {
		Map<String, StreamNode> existedUidMap = new HashMap<>();
		AtomicInteger atomicInteger = new AtomicInteger(0);
		Set<Integer> orderNodeIdSet = new TreeSet<>();
		for (StreamNode streamNode : streamGraph.getStreamNodes()) {
			orderNodeIdSet.add(streamNode.getId());
		}

		for (StreamNode streamNode : streamGraph.getStreamNodes()) {
			if (streamNode.getTransformationUID() != null) {
				existedUidMap.put(streamNode.getTransformationUID(), streamNode);
			}
		}
		for (Integer nodeId : orderNodeIdSet) {
			StreamNode node = streamGraph.getStreamNode(nodeId);
			String uid = "table-" + atomicInteger.getAndIncrement();
			while (existedUidMap.containsKey(uid)) {
				uid = "table-" + atomicInteger.getAndIncrement();
			}
			setNodeUID(node, uid);
		}
		return existedUidMap;
	}

	/**
	 * Clear append uids.
	 */
	public static void clearAppendUids(StreamGraph graph, Map<String, StreamNode> existedUidMap) {
		for (StreamNode node : graph.getStreamNodes()) {
			if (!existedUidMap.containsKey(node.getTransformationUID())) {
				setNodeUID(node, null);
			}
		}
	}

	/**
	 * Set uid.
	 *
	 * @param streamNode StreamNode
	 * @param uid      String
	 */
	public static void setNodeUID(StreamNode streamNode, String uid) {
		try {
			Method method = StreamNode.class.getDeclaredMethod("setTransformationUID",
					String.class);
			method.setAccessible(true);
			method.invoke(streamNode, uid);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("setNodeUID :" + uid + " failed", e);
		}
	}

	/**
	 * Set max parallelism.
	 *
	 * @param streamNode StreamNode
	 * @param paral      Paral
	 */
	public static void setMaxParallelism(StreamNode streamNode, int paral) {
		try {
			Method method = StreamNode.class.getDeclaredMethod("setMaxParallelism",
					int.class);
			method.setAccessible(true);
			method.invoke(streamNode, paral);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("setMaxParallelism paral :" + paral + " failed", e);
		}
	}

	/**
	 * Get max parallelism of stream node.
	 *
	 * @param streamNode StreamNode
	 * @return Max parallelism
	 */
	public static int getMaxParallelism(StreamNode streamNode) {
		try {
			Method method = StreamNode.class.getDeclaredMethod("getMaxParallelism");
			method.setAccessible(true);
			return (int) method.invoke(streamNode);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("getMaxParallelism failed", e);
		}

		return -1;
	}
}

