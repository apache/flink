/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.managementgraph;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.util.ManagementTestUtils;

/**
 * This class contains tests concerning the {@link ManagementGraph} and its components.
 * 
 */
public class ManagementGraphTest {

	/**
	 * This test checks the serialization/deserilization of a management graph.
	 */
	@Test
	public void testManagementGraph() {

		final ManagementGraph orig = constructTestManagementGraph();
		final ManagementGraph copy = (ManagementGraph) ManagementTestUtils.createCopy(orig);

		assertEquals(orig.getJobID(), copy.getJobID());
		assertEquals(orig.getNumberOfStages(), copy.getNumberOfStages());

		for (int i = 0; i < orig.getNumberOfStages(); i++) {

			final ManagementStage origStage = orig.getStage(i);
			final ManagementStage copyStage = copy.getStage(i);

			assertEquals(origStage.getNumberOfGroupVertices(), copyStage.getNumberOfGroupVertices());
			assertEquals(origStage.getNumberOfInputGroupVertices(), copyStage.getNumberOfInputGroupVertices());
			assertEquals(origStage.getNumberOfOutputGroupVertices(), copyStage.getNumberOfOutputGroupVertices());

			for (int j = 0; j < origStage.getNumberOfInputGroupVertices(); j++) {

				final ManagementGroupVertex origGroupVertex = origStage.getInputGroupVertex(j);
				final ManagementGroupVertex copyGroupVertex = copyStage.getInputGroupVertex(j);

				assertEquals(origGroupVertex.getID(), copyGroupVertex.getID());
			}

			for (int j = 0; j < origStage.getNumberOfOutputGroupVertices(); j++) {

				final ManagementGroupVertex origGroupVertex = origStage.getOutputGroupVertex(j);
				final ManagementGroupVertex copyGroupVertex = copyStage.getOutputGroupVertex(j);

				assertEquals(origGroupVertex.getID(), copyGroupVertex.getID());
			}

			for (int j = 0; j < origStage.getNumberOfGroupVertices(); j++) {

				final ManagementGroupVertex origGroupVertex = origStage.getGroupVertex(j);
				final ManagementGroupVertex copyGroupVertex = copyStage.getGroupVertex(j);

				testGroupVertex(origGroupVertex, copyGroupVertex);
			}
		}
	}

	/**
	 * Auxiliary method to test serialization/deserialization of management group vertices.
	 * 
	 * @param origGroupVertex
	 *        the original management group vertex
	 * @param copyGroupVertex
	 *        the deserialized copy of the management group vertex
	 */
	private void testGroupVertex(final ManagementGroupVertex origGroupVertex,
			final ManagementGroupVertex copyGroupVertex) {

		assertEquals(origGroupVertex.getID(), copyGroupVertex.getID());
		assertEquals(origGroupVertex.getName(), copyGroupVertex.getName());
		assertEquals(origGroupVertex.getNumberOfGroupMembers(), copyGroupVertex.getNumberOfGroupMembers());

		for (int k = 0; k < origGroupVertex.getNumberOfForwardEdges(); k++) {

			final ManagementGroupEdge origGroupEdge = origGroupVertex.getForwardEdge(k);
			final ManagementGroupEdge copyGroupEdge = copyGroupVertex.getForwardEdge(k);

			testGroupEdge(origGroupEdge, copyGroupEdge);
		}

		for (int k = 0; k < origGroupVertex.getNumberOfBackwardEdges(); k++) {

			final ManagementGroupEdge origGroupEdge = origGroupVertex.getBackwardEdge(k);
			final ManagementGroupEdge copyGroupEdge = copyGroupVertex.getBackwardEdge(k);

			testGroupEdge(origGroupEdge, copyGroupEdge);
		}

		for (int k = 0; k < origGroupVertex.getNumberOfGroupMembers(); k++) {

			final ManagementVertex origVertex = origGroupVertex.getGroupMember(k);
			final ManagementVertex copyVertex = copyGroupVertex.getGroupMember(k);

			testVertex(origVertex, copyVertex);
		}

	}

	/**
	 * Auxiliary method to test serialization/deserialization of management vertices.
	 * 
	 * @param origVertex
	 *        the original management vertex
	 * @param copyVertex
	 *        the deserialized copy of the management vertex
	 */
	private void testVertex(final ManagementVertex origVertex, final ManagementVertex copyVertex) {

		assertEquals(origVertex.getID(), copyVertex.getID());
		assertEquals(origVertex.getExecutionState(), copyVertex.getExecutionState());
		assertEquals(origVertex.getIndexInGroup(), copyVertex.getIndexInGroup());
		assertEquals(origVertex.getInstanceName(), copyVertex.getInstanceName());
		assertEquals(origVertex.getInstanceType(), copyVertex.getInstanceType());
		assertEquals(origVertex.getNumberOfInputGates(), copyVertex.getNumberOfInputGates());
		assertEquals(origVertex.getNumberOfOutputGates(), copyVertex.getNumberOfOutputGates());

		for (int i = 0; i < origVertex.getNumberOfInputGates(); i++) {

			final ManagementGate origGate = origVertex.getInputGate(i);
			final ManagementGate copyGate = copyVertex.getInputGate(i);

			testGate(origGate, copyGate);
		}

		for (int i = 0; i < origVertex.getNumberOfOutputGates(); i++) {

			final ManagementGate origGate = origVertex.getOutputGate(i);
			final ManagementGate copyGate = copyVertex.getOutputGate(i);

			testGate(origGate, copyGate);
		}
	}

	/**
	 * Auxiliary method to test serialization/deserialization of management gates.
	 * 
	 * @param origGate
	 *        the original management gate
	 * @param copyGate
	 *        the deserialized copy of the management gate
	 */
	private void testGate(final ManagementGate origGate, final ManagementGate copyGate) {

		assertEquals(origGate.getIndex(), copyGate.getIndex());
		assertEquals(origGate.isInputGate(), copyGate.isInputGate());
		assertEquals(origGate.getNumberOfForwardEdges(), copyGate.getNumberOfForwardEdges());
		assertEquals(origGate.getNumberOfBackwardEdges(), copyGate.getNumberOfBackwardEdges());

		for (int i = 0; i < origGate.getNumberOfForwardEdges(); i++) {

			final ManagementEdge origEdge = origGate.getForwardEdge(i);
			final ManagementEdge copyEdge = copyGate.getForwardEdge(i);

			testEdge(origEdge, copyEdge);
		}

		for (int i = 0; i < origGate.getNumberOfBackwardEdges(); i++) {

			final ManagementEdge origEdge = origGate.getBackwardEdge(i);
			final ManagementEdge copyEdge = copyGate.getBackwardEdge(i);

			testEdge(origEdge, copyEdge);
		}
	}

	/**
	 * Auxiliary method to test serialization/deserialization of management group edges.
	 * 
	 * @param origGroupEdge
	 *        the original management group edge
	 * @param copyGroupEdge
	 *        the deserialized copy of the management group edge
	 */
	private void testGroupEdge(final ManagementGroupEdge origGroupEdge, final ManagementGroupEdge copyGroupEdge) {

		assertEquals(origGroupEdge.getChannelType(), copyGroupEdge.getChannelType());
		assertEquals(origGroupEdge.getSourceIndex(), copyGroupEdge.getSourceIndex());
		assertEquals(origGroupEdge.getTargetIndex(), copyGroupEdge.getTargetIndex());
	}

	/**
	 * Auxiliary method to test serialization/deserialization of management edges.
	 * 
	 * @param origEdge
	 *        the original management edge
	 * @param copyEdge
	 *        the deserialized copy of the management edge
	 */
	private void testEdge(final ManagementEdge origEdge, final ManagementEdge copyEdge) {

		assertEquals(origEdge.getChannelType(), copyEdge.getChannelType());
		assertEquals(origEdge.getSourceIndex(), copyEdge.getSourceIndex());
		assertEquals(origEdge.getTargetIndex(), copyEdge.getTargetIndex());
	}

	/**
	 * Constructs a sample management graph that is used during the unit tests.
	 * 
	 * @return the sample management graph used during the tests
	 */
	private static ManagementGraph constructTestManagementGraph() {

		/**
		 * This is the structure of the constructed test graph. The graph
		 * contains two stages and all three channel types.
		 * 4
		 * | In-memory
		 * 3
		 * --/ \-- Network (was FILE)
		 * 2 2
		 * \ / Network
		 * 1
		 */

		// Graph
		final ManagementGraph graph = new ManagementGraph(new JobID());

		// Stages
		final ManagementStage lowerStage = new ManagementStage(graph, 0);
		final ManagementStage upperStage = new ManagementStage(graph, 1);

		// Group vertices
		final ManagementGroupVertex groupVertex1 = new ManagementGroupVertex(lowerStage, "Group Vertex 1");
		final ManagementGroupVertex groupVertex2 = new ManagementGroupVertex(lowerStage, "Group Vertex 2");
		final ManagementGroupVertex groupVertex3 = new ManagementGroupVertex(upperStage, "Group Vertex 3");
		final ManagementGroupVertex groupVertex4 = new ManagementGroupVertex(upperStage, "Group Vertex 4");

		// Vertices
		final ManagementVertex vertex1_1 = new ManagementVertex(groupVertex1, new ManagementVertexID(), "Host 1",
			"small", 0);
		final ManagementVertex vertex2_1 = new ManagementVertex(groupVertex2, new ManagementVertexID(), "Host 2",
			"medium", 0);
		final ManagementVertex vertex2_2 = new ManagementVertex(groupVertex2, new ManagementVertexID(), "Host 2",
			"medium", 1);
		final ManagementVertex vertex3_1 = new ManagementVertex(groupVertex3, new ManagementVertexID(), "Host 2",
			"medium", 0);
		final ManagementVertex vertex4_1 = new ManagementVertex(groupVertex4, new ManagementVertexID(), "Host 2",
			"medium", 0);

		// Input/output gates
		final ManagementGate outputGate1_1 = new ManagementGate(vertex1_1, new ManagementGateID(), 0, false);

		final ManagementGate inputGate2_1 = new ManagementGate(vertex2_1, new ManagementGateID(), 0, true);
		final ManagementGate outputGate2_1 = new ManagementGate(vertex2_1, new ManagementGateID(), 0, false);

		final ManagementGate inputGate2_2 = new ManagementGate(vertex2_2, new ManagementGateID(), 0, true);
		final ManagementGate outputGate2_2 = new ManagementGate(vertex2_2, new ManagementGateID(), 0, false);

		final ManagementGate inputGate3_1 = new ManagementGate(vertex3_1, new ManagementGateID(), 0, true);
		final ManagementGate outputGate3_1 = new ManagementGate(vertex3_1, new ManagementGateID(), 0, false);

		final ManagementGate inputGate4_1 = new ManagementGate(vertex4_1, new ManagementGateID(), 0, true);

		// Group Edges
		new ManagementGroupEdge(groupVertex1, 0, groupVertex2, 0, ChannelType.NETWORK);
		new ManagementGroupEdge(groupVertex2, 0, groupVertex3, 0, ChannelType.NETWORK);
		new ManagementGroupEdge(groupVertex3, 0, groupVertex4, 0, ChannelType.INMEMORY);

		// Edges
		new ManagementEdge(new ManagementEdgeID(), new ManagementEdgeID(), outputGate1_1, 0, inputGate2_1, 0,
			ChannelType.NETWORK);
		new ManagementEdge(new ManagementEdgeID(), new ManagementEdgeID(), outputGate1_1, 1, inputGate2_2, 0,
			ChannelType.NETWORK);
		new ManagementEdge(new ManagementEdgeID(), new ManagementEdgeID(), outputGate2_1, 0, inputGate3_1, 0,
			ChannelType.NETWORK);
		new ManagementEdge(new ManagementEdgeID(), new ManagementEdgeID(), outputGate2_2, 0, inputGate3_1, 1,
			ChannelType.NETWORK);
		new ManagementEdge(new ManagementEdgeID(), new ManagementEdgeID(), outputGate3_1, 0, inputGate4_1, 0,
			ChannelType.INMEMORY);

		return graph;
	}

	/**
	 * This test checks the correctness of the {@link ManagementGraphIterator}. In particular it checks whether the
	 * vertices are visited in the correct order (depth first).
	 */
	@Test
	public void testManagementGraphIterator() {

		final ManagementGraph testGraph = constructTestManagementGraph();

		// Forward traversal
		Iterator<ManagementVertex> it = new ManagementGraphIterator(testGraph, true);

		ManagementVertex[] expectedOrder = new ManagementVertex[5];
		expectedOrder[0] = testGraph.getStage(0).getGroupVertex(0).getGroupMember(0);
		expectedOrder[1] = testGraph.getStage(0).getGroupVertex(1).getGroupMember(0);
		expectedOrder[2] = testGraph.getStage(1).getGroupVertex(0).getGroupMember(0);
		expectedOrder[3] = testGraph.getStage(1).getGroupVertex(1).getGroupMember(0);
		expectedOrder[4] = testGraph.getStage(0).getGroupVertex(1).getGroupMember(1);

		checkManagementVertexOrder(it, expectedOrder);

		it = new ManagementGraphIterator(testGraph, 0, false, true);

		checkManagementVertexOrder(it, expectedOrder);

		it = new ManagementGraphIterator(testGraph, expectedOrder[0], true);

		checkManagementVertexOrder(it, expectedOrder);

		expectedOrder = new ManagementVertex[3];
		expectedOrder[0] = testGraph.getStage(0).getGroupVertex(0).getGroupMember(0);
		expectedOrder[1] = testGraph.getStage(0).getGroupVertex(1).getGroupMember(0);
		expectedOrder[2] = testGraph.getStage(0).getGroupVertex(1).getGroupMember(1);

		it = new ManagementGraphIterator(testGraph, 0, true, true);

		checkManagementVertexOrder(it, expectedOrder);

		// Backward traversal
		it = new ManagementGraphIterator(testGraph, false);

		expectedOrder = new ManagementVertex[5];
		expectedOrder[0] = testGraph.getStage(1).getGroupVertex(1).getGroupMember(0);
		expectedOrder[1] = testGraph.getStage(1).getGroupVertex(0).getGroupMember(0);
		expectedOrder[2] = testGraph.getStage(0).getGroupVertex(1).getGroupMember(0);
		expectedOrder[3] = testGraph.getStage(0).getGroupVertex(0).getGroupMember(0);
		expectedOrder[4] = testGraph.getStage(0).getGroupVertex(1).getGroupMember(1);

		checkManagementVertexOrder(it, expectedOrder);

		it = new ManagementGraphIterator(testGraph, 1, true, false);

		expectedOrder = new ManagementVertex[2];
		expectedOrder[0] = testGraph.getStage(1).getGroupVertex(1).getGroupMember(0);
		expectedOrder[1] = testGraph.getStage(1).getGroupVertex(0).getGroupMember(0);

		checkManagementVertexOrder(it, expectedOrder);
	}

	/**
	 * This test checks the correctness of the {@link ManagementGroupVertexIterator}. In particular it checks whether
	 * the
	 * vertices are visited in the correct order (depth first).
	 */
	@Test
	public void testManagementGroupVertexIterator() {

		final ManagementGraph testGraph = constructTestManagementGraph();

		Iterator<ManagementGroupVertex> it = new ManagementGroupVertexIterator(testGraph, true, -1);
		ManagementGroupVertex[] expectedOrder = new ManagementGroupVertex[4];
		expectedOrder[0] = testGraph.getStage(0).getGroupVertex(0);
		expectedOrder[1] = testGraph.getStage(0).getGroupVertex(1);
		expectedOrder[2] = testGraph.getStage(1).getGroupVertex(0);
		expectedOrder[3] = testGraph.getStage(1).getGroupVertex(1);

		checkManagementGroupVertexOrder(it, expectedOrder);

		it = new ManagementGroupVertexIterator(testGraph, false, -1);
		expectedOrder[0] = testGraph.getStage(1).getGroupVertex(1);
		expectedOrder[1] = testGraph.getStage(1).getGroupVertex(0);
		expectedOrder[2] = testGraph.getStage(0).getGroupVertex(1);
		expectedOrder[3] = testGraph.getStage(0).getGroupVertex(0);

		checkManagementGroupVertexOrder(it, expectedOrder);

		it = new ManagementGroupVertexIterator(testGraph, true, 0);
		expectedOrder = new ManagementGroupVertex[2];
		expectedOrder[0] = testGraph.getStage(0).getGroupVertex(0);
		expectedOrder[1] = testGraph.getStage(0).getGroupVertex(1);

		checkManagementGroupVertexOrder(it, expectedOrder);

		it = new ManagementGroupVertexIterator(testGraph, false, 1);
		expectedOrder[0] = testGraph.getStage(1).getGroupVertex(1);
		expectedOrder[1] = testGraph.getStage(1).getGroupVertex(0);

		checkManagementGroupVertexOrder(it, expectedOrder);
	}

	/**
	 * Auxiliary method to check if the order in which the management vertices are visited by the given iterator
	 * corresponds to the one in the given array.
	 * 
	 * @param it
	 *        the iterator to be used to traverse the vertices
	 * @param expectedOrder
	 *        array with the expected order of vertices
	 */
	private void checkManagementVertexOrder(final Iterator<ManagementVertex> it, final ManagementVertex[] expectedOrder) {

		int i = 0;
		while (it.hasNext()) {

			final ManagementVertex vertex = it.next();
			assertEquals(expectedOrder[i++], vertex);
		}

		assertEquals(expectedOrder.length, i);
	}

	/**
	 * Auxiliary method to check if the order in which the management group vertices are visited by the given iterator
	 * corresponds to the one in the given array.
	 * 
	 * @param it
	 *        the iterator to be used to traverse the vertices
	 * @param expectedOrder
	 *        array with the expected order of vertices
	 */
	private void checkManagementGroupVertexOrder(final Iterator<ManagementGroupVertex> it,
			final ManagementGroupVertex[] expectedOrder) {

		int i = 0;
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			assertEquals(expectedOrder[i++], groupVertex);
		}

		assertEquals(expectedOrder.length, i);
	}
}
