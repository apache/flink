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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.examples.java.clustering.KMeans;
import org.apache.flink.examples.java.clustering.util.KMeansData;
import org.apache.flink.examples.java.graph.ConnectedComponents;
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test that runs an iterative job after a failure in another iterative job.
 * This test validates that task slots in co-location constraints are properly
 * freed in the presence of failures.
 */
public class SuccessAfterNetworkBuffersFailureITCase extends TestLogger {

	private static final int PARALLELISM = 16;

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfiguration())
			.setNumberTaskManagers(2)
			.setNumberSlotsPerTaskManager(8)
			.build());

	private static Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("80m"));
		config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.ofMebiBytes(25L));
		config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(25L));
		return config;
	}

	@Test
	public void testSuccessfulProgramAfterFailure() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		runConnectedComponents(env);

		try {
			runKMeans(env);
			fail("This program execution should have failed.");
		}
		catch (JobExecutionException e) {
			assertTrue(findThrowableWithMessage(e, "Insufficient number of network buffers").isPresent());
		}

		runConnectedComponents(env);
	}

	private static void runConnectedComponents(ExecutionEnvironment env) throws Exception {

		env.setParallelism(PARALLELISM);

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

	private static void runKMeans(ExecutionEnvironment env) throws Exception {

		env.setParallelism(PARALLELISM);

		// get input data
		DataSet<KMeans.Point> points =  KMeansData.getDefaultPointDataSet(env).rebalance();
		DataSet<KMeans.Centroid> centroids =  KMeansData.getDefaultCentroidDataSet(env).rebalance();

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<KMeans.Centroid> loop = centroids.iterate(20);

		// add some re-partitions to increase network buffer use
		DataSet<KMeans.Centroid> newCentroids = points
				// compute closest centroid for each point
				.map(new KMeans.SelectNearestCenter()).withBroadcastSet(loop, "centroids")
				.rebalance()
				// count and sum point coordinates for each centroid
				.map(new KMeans.CountAppender())
				.groupBy(0).reduce(new KMeans.CentroidAccumulator())
				// compute new centroids from point counts and coordinate sums
				.rebalance()
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
