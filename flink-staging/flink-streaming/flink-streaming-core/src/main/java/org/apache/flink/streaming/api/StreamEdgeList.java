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

package org.apache.flink.streaming.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StreamEdgeList {

	private Map<Integer, List<StreamEdge>> outEdgeLists;
	private Map<Integer, List<StreamEdge>> inEdgeLists;

	public StreamEdgeList() {
		outEdgeLists = new HashMap<Integer, List<StreamEdge>>();
		inEdgeLists = new HashMap<Integer, List<StreamEdge>>();
	}

	public void addVertex(int vertexId) {
		outEdgeLists.put(vertexId, new ArrayList<StreamEdge>());
		inEdgeLists.put(vertexId, new ArrayList<StreamEdge>());
	}

	public void removeVertex(int vertexId) {
		ArrayList<StreamEdge> toRemove = new ArrayList<StreamEdge>();

		for (StreamEdge edge : outEdgeLists.get(vertexId)) {
			toRemove.add(edge);
		}

		for (StreamEdge edge : inEdgeLists.get(vertexId)) {
			toRemove.add(edge);
		}

		for (StreamEdge edge : toRemove) {
			removeEdge(edge);
		}

		outEdgeLists.remove(vertexId);
		inEdgeLists.remove(vertexId);
	}

	public void addEdge(StreamEdge edge) {
		int sourceId = edge.getSourceVertex();
		int targetId = edge.getTargetVertex();
		outEdgeLists.get(sourceId).add(edge);
		inEdgeLists.get(targetId).add(edge);
	}

	public void removeEdge(StreamEdge edge) {
		int sourceId = edge.getSourceVertex();
		int targetId = edge.getTargetVertex();
		removeEdge(sourceId, targetId);
	}

	public void removeEdge(int sourceId, int targetId) {
		Iterator<StreamEdge> outIterator = outEdgeLists.get(sourceId).iterator();
		while (outIterator.hasNext()) {
			StreamEdge edge = outIterator.next();

			if (edge.getTargetVertex() == targetId) {
				outIterator.remove();
			}
		}

		Iterator<StreamEdge> inIterator = inEdgeLists.get(targetId).iterator();
		while (inIterator.hasNext()) {
			StreamEdge edge = inIterator.next();

			if (edge.getSourceVertex() == sourceId) {
				inIterator.remove();
			}
		}
	}

	public List<StreamEdge> getOutEdges(int i) {
		List<StreamEdge> outEdges = outEdgeLists.get(i);

		if (outEdges == null) {
			throw new RuntimeException("No such vertex in stream graph: " + i);
		}

		return outEdges;
	}

	public List<StreamEdge> getInEdges(int i) {
		List<StreamEdge> inEdges = inEdgeLists.get(i);

		if (inEdges == null) {
			throw new RuntimeException("No such vertex in stream graph: " + i);
		}

		return inEdges;
	}
}