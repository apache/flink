/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * GraphState represents a special {@link MapState} for storing graph
 * structures.
 *
 */
public class GraphState extends MapState<Integer, Set<Integer>> {

	private static final long serialVersionUID = 1L;

	public GraphState() {
		state = new HashMap<Integer, Set<Integer>>();
	}

	public void insertDirectedEdge(int sourceNode, int targetNode) {
		if (!containsKey(sourceNode)) {
			state.put(sourceNode, new HashSet<Integer>());
		}
		state.get(sourceNode).add(targetNode);
		updatedItems.add(sourceNode);
		removedItems.remove(sourceNode);
	}

	public void insertUndirectedEdge(int sourceNode, int targetNode) {
		if (!state.containsKey(sourceNode)) {
			state.put(sourceNode, new HashSet<Integer>());
		}
		if (!state.containsKey(targetNode)) {
			state.put(targetNode, new HashSet<Integer>());
		}
		state.get(sourceNode).add(targetNode);
		state.get(targetNode).add(sourceNode);

		updatedItems.add(sourceNode);
		removedItems.remove(sourceNode);
		updatedItems.add(targetNode);
		removedItems.remove(targetNode);
	}
}
