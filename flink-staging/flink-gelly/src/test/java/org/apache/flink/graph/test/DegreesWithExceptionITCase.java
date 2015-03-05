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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.junit.Assert;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.fail;

public class DegreesWithExceptionITCase {

	@Test
	public void testOutDegreesInvalidEdgeSrcId() throws Exception {
		/*
		* Test outDegrees() with an edge having a srcId that does not exist in the vertex DataSet
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidSrcData(env), env);

		try {
			graph.outDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.outDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			Assert.assertEquals("The edge src/trg id could not be found within the vertexIds", e.getCause().getMessage());
			Assert.assertTrue(e.getCause() instanceof NoSuchElementException);
		}
	}

	@Test
	public void testInDegreesInvalidEdgeTrgId() throws Exception {
		/*
		* Test inDegrees() with an edge having a trgId that does not exist in the vertex DataSet
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidTrgData(env), env);

		try {
			graph.inDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.inDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			Assert.assertEquals("The edge src/trg id could not be found within the vertexIds", e.getCause().getMessage());
			Assert.assertTrue(e.getCause() instanceof NoSuchElementException);
		}
	}

	@Test
	public void testGetDegreesInvalidEdgeTrgId() throws Exception {
		/*
		* Test getDegrees() with an edge having a trgId that does not exist in the vertex DataSet
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidTrgData(env), env);

		try {
			graph.getDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.getDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			Assert.assertEquals("The edge src/trg id could not be found within the vertexIds", e.getCause().getMessage());
			Assert.assertTrue(e.getCause() instanceof NoSuchElementException);
		}
	}

	@Test
	public void testGetDegreesInvalidEdgeSrcId() throws Exception {
		/*
		* Test getDegrees() with an edge having a srcId that does not exist in the vertex DataSet
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidSrcData(env), env);

		try {
			graph.getDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.getDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			Assert.assertEquals("The edge src/trg id could not be found within the vertexIds", e.getCause().getMessage());
			Assert.assertTrue(e.getCause() instanceof NoSuchElementException);
		}
	}

	@Test
	public void testGetDegreesInvalidEdgeSrcTrgId() throws Exception {
		/*
		* Test getDegrees() with an edge having a srcId and a trgId that does not exist in the vertex DataSet
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeInvalidSrcTrgData(env), env);

		try {
			graph.getDegrees().output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			env.execute();

			fail("graph.getDegrees() did not throw NoSuchElementException");
		} catch (Exception e) {
			Assert.assertEquals("The edge src/trg id could not be found within the vertexIds", e.getCause().getMessage());
			Assert.assertTrue(e.getCause() instanceof NoSuchElementException);
		}
	}
}
