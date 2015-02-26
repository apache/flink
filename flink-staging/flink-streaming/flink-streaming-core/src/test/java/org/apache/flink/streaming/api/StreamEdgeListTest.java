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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class StreamEdgeListTest {

	private StreamEdgeList edgeList;

	@Before
	public void init() {
		edgeList = new StreamEdgeList();
	}

	@Test
	public void test() {
		edgeList.addVertex(1);
		edgeList.addVertex(2);
		edgeList.addVertex(3);


		// add edges
		StreamEdge edge1 = new StreamEdge(1, 2, -1, null, null);
		StreamEdge edge2 = new StreamEdge(2, 3, -1, null, null);
		StreamEdge edge3 = new StreamEdge(1, 3, -1, null, null);

		edgeList.addEdge(edge1);
		edgeList.addEdge(edge2);
		edgeList.addEdge(edge3);

		// check adding
		checkIfSameElements(edgeList.getOutEdges(1), Arrays.asList(edge1, edge3));
		checkIfSameElements(edgeList.getOutEdges(2), Arrays.asList(edge2));
		checkIfSameElements(edgeList.getOutEdges(3), new ArrayList<StreamEdge>());

		checkIfSameElements(edgeList.getInEdges(1), new ArrayList<StreamEdge>());
		checkIfSameElements(edgeList.getInEdges(2), Arrays.asList(edge1));
		checkIfSameElements(edgeList.getInEdges(3), Arrays.asList(edge2, edge3));

		// add duplicate edges
		StreamEdge edge1new = new StreamEdge(1, 2, -2, null, null);
		StreamEdge edge2new = new StreamEdge(2, 3, -2, null, null);

		edgeList.addEdge(edge1new);
		edgeList.addEdge(edge2new);

		// check adding
		checkIfSameElements(edgeList.getOutEdges(1), Arrays.asList(edge1, edge1new, edge3));
		checkIfSameElements(edgeList.getOutEdges(2), Arrays.asList(edge2, edge2new));
		checkIfSameElements(edgeList.getOutEdges(3), new ArrayList<StreamEdge>());

		checkIfSameElements(edgeList.getInEdges(1), new ArrayList<StreamEdge>());
		checkIfSameElements(edgeList.getInEdges(2), Arrays.asList(edge1, edge1new));
		checkIfSameElements(edgeList.getInEdges(3), Arrays.asList(edge2, edge2new, edge3));

		// remove a duplicate edge
		edgeList.removeEdge(1, 2);

		// check removing
		checkIfSameElements(edgeList.getOutEdges(1), Arrays.asList(edge3));
		checkIfSameElements(edgeList.getOutEdges(2), Arrays.asList(edge2, edge2new));

		checkIfSameElements(edgeList.getInEdges(1), new ArrayList<StreamEdge>());
		checkIfSameElements(edgeList.getInEdges(2), new ArrayList<StreamEdge>());

		// add back an edge and delete a vertex
		edgeList.addEdge(edge1);
		edgeList.removeVertex(2);

		// check removing
		checkIfSameElements(edgeList.getOutEdges(1), Arrays.asList(edge3));
		try {
			checkIfSameElements(edgeList.getOutEdges(2), null);
			fail();
		} catch (RuntimeException e) {
		}
		checkIfSameElements(edgeList.getOutEdges(3), new ArrayList<StreamEdge>());

		checkIfSameElements(edgeList.getInEdges(1), new ArrayList<StreamEdge>());
		try {
			checkIfSameElements(edgeList.getInEdges(2), null);
			fail();
		} catch (RuntimeException e) {
		}
		checkIfSameElements(edgeList.getInEdges(3), Arrays.asList(edge3));
	}

	private <T> void checkIfSameElements(List<T> expected, List<T> result) {
		assertEquals(new HashSet<T>(expected), new HashSet<T>(result));
	}
}