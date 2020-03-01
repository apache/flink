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

package org.apache.flink.test.optimizer.examples;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.optimizer.util.OperatorResolver;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Validate the compilation and result of a single iteration of KMeans.
 */
@SuppressWarnings("serial")
public class KMeansSingleStepTest extends CompilerTestBase {

	private static final String DATAPOINTS = "Data Points";
	private static final String CENTERS = "Centers";

	private static final String MAPPER_NAME = "Find Nearest Centers";
	private static final String REDUCER_NAME = "Recompute Center Positions";

	private static final String SINK = "New Center Positions";

	private final FieldList set0 = new FieldList(0);

	@Test
	public void testCompileKMeansSingleStepWithStats() throws Exception {

		Plan p = getKMeansPlan();
		p.setExecutionConfig(new ExecutionConfig());
		// set the statistics
		OperatorResolver cr = getContractResolver(p);
		GenericDataSourceBase<?, ?> pointsSource = cr.getNode(DATAPOINTS);
		GenericDataSourceBase<?, ?> centersSource = cr.getNode(CENTERS);
		setSourceStatistics(pointsSource, 100L * 1024 * 1024 * 1024, 32f);
		setSourceStatistics(centersSource, 1024 * 1024, 32f);

		OptimizedPlan plan = compileWithStats(p);
		checkPlan(plan);
	}

	@Test
	public void testCompileKMeansSingleStepWithOutStats() throws Exception {
		Plan p = getKMeansPlan();
		p.setExecutionConfig(new ExecutionConfig());
		OptimizedPlan plan = compileNoStats(p);
		checkPlan(plan);
	}

	private void checkPlan(OptimizedPlan plan) {

		OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);

		final SinkPlanNode sink = or.getNode(SINK);
		final SingleInputPlanNode reducer = or.getNode(REDUCER_NAME);
		final SingleInputPlanNode combiner = (SingleInputPlanNode) reducer.getPredecessor();
		final SingleInputPlanNode mapper = or.getNode(MAPPER_NAME);

		// check the mapper
		assertEquals(1, mapper.getBroadcastInputs().size());
		assertEquals(ShipStrategyType.FORWARD, mapper.getInput().getShipStrategy());
		assertEquals(ShipStrategyType.BROADCAST, mapper.getBroadcastInputs().get(0).getShipStrategy());

		assertEquals(LocalStrategy.NONE, mapper.getInput().getLocalStrategy());
		assertEquals(LocalStrategy.NONE, mapper.getBroadcastInputs().get(0).getLocalStrategy());

		assertEquals(DriverStrategy.MAP, mapper.getDriverStrategy());

		assertNull(mapper.getInput().getLocalStrategyKeys());
		assertNull(mapper.getInput().getLocalStrategySortOrder());
		assertNull(mapper.getBroadcastInputs().get(0).getLocalStrategyKeys());
		assertNull(mapper.getBroadcastInputs().get(0).getLocalStrategySortOrder());

		// check the combiner
		Assert.assertNotNull(combiner);
		assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
		assertEquals(LocalStrategy.NONE, combiner.getInput().getLocalStrategy());
		assertEquals(DriverStrategy.SORTED_GROUP_COMBINE, combiner.getDriverStrategy());
		assertNull(combiner.getInput().getLocalStrategyKeys());
		assertNull(combiner.getInput().getLocalStrategySortOrder());
		assertEquals(set0, combiner.getKeys(0));
		assertEquals(set0, combiner.getKeys(1));

		// check the reducer
		assertEquals(ShipStrategyType.PARTITION_HASH, reducer.getInput().getShipStrategy());
		assertEquals(LocalStrategy.COMBININGSORT, reducer.getInput().getLocalStrategy());
		assertEquals(DriverStrategy.SORTED_GROUP_REDUCE, reducer.getDriverStrategy());
		assertEquals(set0, reducer.getKeys(0));
		assertEquals(set0, reducer.getInput().getLocalStrategyKeys());
		assertTrue(Arrays.equals(reducer.getInput().getLocalStrategySortOrder(), reducer.getSortOrders(0)));

		// check the sink
		assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		assertEquals(LocalStrategy.NONE, sink.getInput().getLocalStrategy());
	}

	public static Plan getKMeansPlan() throws Exception {
		return kmeans(new String[]{IN_FILE, IN_FILE, OUT_FILE, "20"});
	}

	public static Plan kmeans(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Point> points = env.readCsvFile(args[0])
				.fieldDelimiter(" ")
				.includeFields(true, true)
				.types(Double.class, Double.class)
				.name(DATAPOINTS)
				.map(new MapFunction<Tuple2<Double, Double>, Point>() {
					@Override
					public Point map(Tuple2<Double, Double> value) throws Exception {
						return new Point(value.f0, value.f1);
					}
				});

		DataSet<Centroid> centroids = env.readCsvFile(args[1])
				.fieldDelimiter(" ")
				.includeFields(true, true, true)
				.types(Integer.class, Double.class, Double.class)
				.name(CENTERS)
				.map(new MapFunction<Tuple3<Integer, Double, Double>, Centroid>() {
					@Override
					public Centroid map(Tuple3<Integer, Double, Double> value) throws Exception {
						return new Centroid(value.f0, value.f1, value.f2);
					}
				});

		DataSet<Tuple3<Integer, Point, Integer>> newCentroids = points
				.map(new SelectNearestCenter()).name(MAPPER_NAME).withBroadcastSet(centroids, "centroids");

		DataSet<Tuple3<Integer, Point, Integer>> recomputeClusterCenter =
				newCentroids.groupBy(0).reduceGroup(new RecomputeClusterCenter()).name(REDUCER_NAME);

		recomputeClusterCenter.project(0, 1).writeAsCsv(args[2], "\n", " ").name(SINK);

		return env.createProgramPlan("KMeans Example");
	}

	/**
	 * Two-dimensional point.
	 */
	public static class Point extends Tuple2<Double, Double> {
		public Point(double x, double y) {
			this.f0 = x;
			this.f1 = y;
		}

		public Point add(Point other) {
			f0 += other.f0;
			f1 += other.f1;
			return this;
		}

		public Point div(long val) {
			f0 /= val;
			f1 /= val;
			return this;
		}

		public double euclideanDistance(Point other) {
			return Math.sqrt((f0 - other.f0) * (f0 - other.f0) + (f1 - other.f1) * (f1 - other.f1));
		}

		public double euclideanDistance(Centroid other) {
			return Math.sqrt((f0 - other.f1.f0) * (f0 - other.f1.f0) + (f1 - other.f1.f1) * (f1 - other.f1.f1));
		}
	}

	/**
	 * Center of a cluster.
	 */
	public static class Centroid extends Tuple2<Integer, Point> {

		public Centroid(int id, double x, double y) {
			this.f0 = id;
			this.f1 = new Point(x, y);
		}

		public Centroid(int id, Point p) {
			this.f0 = id;
			this.f1 = p;
		}
	}

	/**
	 * Determines the closest cluster center for a data point.
	 */
	private static final class SelectNearestCenter extends RichMapFunction<Point, Tuple3<Integer, Point, Integer>> {
		private Collection<Centroid> centroids;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple3<Integer, Point, Integer> map(Point p) throws Exception {
			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;
			for (Centroid centroid : centroids) {
				double distance = p.euclideanDistance(centroid);
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.f0;
				}
			}
			return new Tuple3<>(closestCentroidId, p, 1);
		}
	}

	private static final class RecomputeClusterCenter implements
		GroupReduceFunction<Tuple3<Integer, Point, Integer>, Tuple3<Integer, Point, Integer>>,
		GroupCombineFunction<Tuple3<Integer, Point, Integer>, Tuple3<Integer, Point, Integer>> {

		@Override
		public void reduce(Iterable<Tuple3<Integer, Point, Integer>> values, Collector<Tuple3<Integer, Point, Integer>> out) throws Exception {
			int id = -1;
			double x = 0;
			double y = 0;
			int count = 0;
			for (Tuple3<Integer, Point, Integer> value : values) {
				id = value.f0;
				x += value.f1.f0;
				y += value.f1.f1;
				count += value.f2;
			}
			out.collect(new Tuple3<>(id, new Point(x, y), count));
		}

		@Override
		public void combine(Iterable<Tuple3<Integer, Point, Integer>> values, Collector<Tuple3<Integer, Point, Integer>> out) throws Exception {
			reduce(values, out);
		}
	}
}
