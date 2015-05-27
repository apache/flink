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

package org.apache.flink.test.manual;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.graph.ConnectedComponents;
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;

import static org.junit.Assert.fail;

/**
 * This test starts a mini cluster with 100 task managers and runs connected components
 * with a parallelism of 100.
 */
public class NotSoMiniClusterIterations {
	
	private static final int PARALLELISM = 100;
	
	public static void main(String[] args) {
		if ((Runtime.getRuntime().maxMemory() >>> 20) < 5000) {
			throw new RuntimeException("This test program needs to run with at least 5GB of heap space.");
		}
		
		LocalFlinkMiniCluster cluster = null;

		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, PARALLELISM);
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 8);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
			config.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 1000);
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY, 8 * 1024);
			
			config.setInteger("taskmanager.net.server.numThreads", 1);
			config.setInteger("taskmanager.net.client.numThreads", 1);

			cluster = new LocalFlinkMiniCluster(config, false);

			runConnectedComponents(cluster.getJobManagerRPCPort());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (cluster != null) {
				cluster.shutdown();
			}
		}
	}

	private static void runConnectedComponents(int jmPort) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", jmPort);
		env.setParallelism(PARALLELISM);
		env.getConfig().disableSysoutLogging();

		// read vertex and edge data
		DataSet<Long> vertices = ConnectedComponentsData.getDefaultVertexDataSet(env)
				.rebalance();

		DataSet<Tuple2<Long, Long>> edges = ConnectedComponentsData.getDefaultEdgeDataSet(env)
				.rebalance()
				.flatMap(new ConnectedComponents.UndirectEdge());

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices
				.map(new ConnectedComponents.DuplicateValue<Long>());

		// open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
				verticesWithInitialId.iterateDelta(verticesWithInitialId, 100, 0);

		// apply the step logic: join with the edges, select the minimum neighbor,
		// update if the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset().join(edges)
				.where(0).equalTo(0)
				.with(new ConnectedComponents.NeighborWithComponentIDJoin())

				.groupBy(0).aggregate(Aggregations.MIN, 1)

				.join(iteration.getSolutionSet())
				.where(0).equalTo(0)
				.with(new ConnectedComponents.ComponentIdFilter());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);

		result.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

		env.execute();
	}
}
