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

package org.apache.flink.graph.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Dummy iteration to test that the supersteps are correctly incremented
 * and can be retrieved from inside the scatter and gather functions.
 * All vertices start with value 1 and increase their value by 1
 * in each iteration.
 */
@SuppressWarnings("serial")
public class CollectionModeSuperstepITCase extends TestLogger {

	@Test
	public void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromCollection(TestGraphUtils.getLongLongVertices(),
				TestGraphUtils.getLongLongEdges(), env).mapVertices(new AssignOneMapper());

		Graph<Long, Long, Long> result = graph.runScatterGatherIteration(
				new MessageFunction(), new UpdateFunction(), 10);

		result.getVertices().map(
			new VertexToTuple2Map<>()).output(
				new DiscardingOutputFormat<>());

		env.execute();
	}

	private static final class MessageFunction extends ScatterFunction<Long, Long, Long, Long> {
		@Override
		public void sendMessages(Vertex<Long, Long> vertex) {
			long superstep = getSuperstepNumber();
			Assert.assertEquals(true, vertex.getValue() == superstep);
			//send message to keep vertices active
			sendMessageToAllNeighbors(vertex.getValue());
		}
	}

	private static final class UpdateFunction extends GatherFunction<Long, Long, Long> {
		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
			long superstep = getSuperstepNumber();
			Assert.assertEquals(true, vertex.getValue() == superstep);
			setNewVertexValue(vertex.getValue() + 1);
		}
	}

	private static final class AssignOneMapper implements MapFunction<Vertex<Long, Long>, Long> {
		public Long map(Vertex<Long, Long> value) {
			return 1L;
		}
	}
}
