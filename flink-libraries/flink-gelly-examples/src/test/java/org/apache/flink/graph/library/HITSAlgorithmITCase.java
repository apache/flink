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

package org.apache.flink.graph.library;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.HITSAlgorithm;
import org.apache.flink.graph.examples.data.HITSData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.NullValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class HITSAlgorithmITCase extends MultipleProgramsTestBase{
	
	public HITSAlgorithmITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testHITSWithTenIterations() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Double, NullValue> graph = Graph.fromDataSet(
				HITSData.getVertexDataSet(env),
				HITSData.getEdgeDataSet(env),
				env);
		
		List<Vertex<Long, Tuple2<DoubleValue, DoubleValue>>> result = graph.run(new HITSAlgorithm<Long, Double, NullValue>(10)).collect();

		compareWithDelta(result, 1e-7);
	}

	@Test
	public void testHITSWithConvergeThreshold() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Double, NullValue> graph = Graph.fromDataSet(
				HITSData.getVertexDataSet(env),
				HITSData.getEdgeDataSet(env),
				env);

		List<Vertex<Long, Tuple2<DoubleValue, DoubleValue>>> result = graph.run(new HITSAlgorithm<Long, Double, NullValue>(1e-7)).collect();

		compareWithDelta(result, 1e-7);
	}

	private void compareWithDelta(List<Vertex<Long, Tuple2<DoubleValue, DoubleValue>>> result, double delta) {

		String resultString = "";
		for (Vertex<Long, Tuple2<DoubleValue, DoubleValue>> v : result) {
			resultString += v.f0.toString() + "," + v.f1.f0.toString() + "," + v.f1.f1.toString() +"\n";
		}

		String expectedResult = HITSData.VALUE_AFTER_10_ITERATIONS;
		
		String[] expected = expectedResult.isEmpty() ? new String[0] : expectedResult.split("\n");

		String[] resultArray = resultString.isEmpty() ? new String[0] : resultString.split("\n");

		Arrays.sort(expected);
		Arrays.sort(resultArray);

		for (int i = 0; i < expected.length; i++) {
			String[] expectedFields = expected[i].split(",");
			String[] resultFields = resultArray[i].split(",");

			double expectedHub = Double.parseDouble(expectedFields[1]);
			double resultHub = Double.parseDouble(resultFields[1]);
			
			double expectedAuthority = Double.parseDouble(expectedFields[2]);
			double resultAuthority = Double.parseDouble(resultFields[2]);

			Assert.assertTrue("Values differ by more than the permissible delta",
					Math.abs(expectedHub - resultHub) < delta);

			Assert.assertTrue("Values differ by more than the permissible delta",
					Math.abs(expectedAuthority - resultAuthority) < delta);
		}
	}
}
