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

package org.apache.flink.test.misc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.clustering.KMeans;
import org.apache.flink.examples.java.clustering.util.KMeansData;
import org.apache.flink.examples.java.graph.ConnectedComponents;
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import org.junit.Test;

import static org.junit.Assert.*;

public class SuccessAfterNetworkBuffersFailureITCase {
	
	
	@Test
	public void testSuccessfulProgramAfterFailure() {
		ForkableFlinkMiniCluster cluster = null;
		
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 2);
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 80);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);
			config.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 840);
			
			cluster = new ForkableFlinkMiniCluster(config, false);
			
			try {
				runConnectedComponents(cluster.getJobManagerRPCPort());
			}
			catch (Exception e) {
				e.printStackTrace();
				fail("Program Execution should have succeeded.");
			}
	
			try {
				runKMeans(cluster.getJobManagerRPCPort());
				fail("This program execution should have failed.");
			}
			catch (ProgramInvocationException e) {
				assertTrue(e.getCause().getCause().getMessage().contains("Insufficient number of network buffers"));
			}
	
			try {
				runConnectedComponents(cluster.getJobManagerRPCPort());
			}
			catch (Exception e) {
				e.printStackTrace();
				fail("Program Execution should have succeeded.");
			}
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
		env.setParallelism(16);
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

	private static void runKMeans(int jmPort) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", jmPort);
		env.setParallelism(16);
		env.getConfig().disableSysoutLogging();

		// get input data
		DataSet<KMeans.Point> points =  KMeansData.getDefaultPointDataSet(env).rebalance();
		DataSet<KMeans.Centroid> centroids =  KMeansData.getDefaultCentroidDataSet(env).rebalance();

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<KMeans.Centroid> loop = centroids.iterate(20);

		DataSet<KMeans.Centroid> newCentroids = points
				// compute closest centroid for each point
				.map(new KMeans.SelectNearestCenter()).withBroadcastSet(loop, "centroids")
						// count and sum point coordinates for each centroid
				.map(new KMeans.CountAppender())
				.groupBy(0).reduce(new KMeans.CentroidAccumulator())
						// compute new centroids from point counts and coordinate sums
				.map(new KMeans.CentroidAverager());

		// feed new centroids back into next iteration
		DataSet<KMeans.Centroid> finalCentroids = loop.closeWith(newCentroids);

		DataSet<Tuple2<Integer, KMeans.Point>> clusteredPoints = points
				// assign points to final clusters
				.map(new KMeans.SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

		clusteredPoints.output(new DiscardingOutputFormat<Tuple2<Integer, KMeans.Point>>());
		
		env.execute("KMeans Example");
	}
}
