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

package org.apache.flink.test.classloading.jar;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * This class belongs to the {@link org.apache.flink.test.classloading.ClassLoaderITCase} test.
 *
 * <p>It tests dynamic class loading for:
 * <ul>
 *     <li>Custom Functions</li>
 *     <li>Custom Data Types</li>
 *     <li>Custom Accumulators</li>
 *     <li>Custom Types in collect()</li>
 * </ul>
 *
 * <p>It's removed by Maven from classpath, so other tests must not depend on it.
 */
@SuppressWarnings("serial")
public class KMeansForTest {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			throw new IllegalArgumentException("Missing parameters");
		}

		final String pointsData = args[0];
		final String centersData = args[1];
		final int numIterations = Integer.parseInt(args[2]);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Point> points = env.fromElements(pointsData.split("\n"))
				.map(new TuplePointConverter());

		DataSet<Centroid> centroids = env.fromElements(centersData.split("\n"))
				.map(new TupleCentroidConverter());

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Centroid> loop = centroids.iterate(numIterations);

		DataSet<Centroid> newCentroids = points
				// compute closest centroid for each point
				.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")

				// count and sum point coordinates for each centroid (test pojo return type)
				.map(new CountAppender())

				// !test if key expressions are working!
				.groupBy("field0").reduce(new CentroidAccumulator())

				// compute new centroids from point counts and coordinate sums
				.map(new CentroidAverager());

		// feed new centroids back into next iteration
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

		// test that custom data type collects are working
		finalCentroids.collect();
	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	/**
	 * A simple two-dimensional point.
	 */
	public static class Point {

		public double x, y;

		public Point() {}

		public Point(double x, double y) {
			this.x = x;
			this.y = y;
		}

		public Point add(Point other) {
			x += other.x;
			y += other.y;
			return this;
		}

		public Point div(long val) {
			x /= val;
			y /= val;
			return this;
		}

		public double euclideanDistance(Point other) {
			return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
		}

		public void clear() {
			x = y = 0.0;
		}

		@Override
		public String toString() {
			return x + " " + y;
		}
	}

	/**
	 * A simple two-dimensional centroid, basically a point with an ID.
	 */
	public static class Centroid extends Point {

		public int id;

		public Centroid() {}

		public Centroid(int id, double x, double y) {
			super(x, y);
			this.id = id;
		}

		public Centroid(int id, Point p) {
			super(p.x, p.y);
			this.id = id;
		}

		@Override
		public String toString() {
			return id + " " + super.toString();
		}
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Converts a {@code Tuple2<Double, Double>} into a {@link Point}.
	 */
	public static final class TuplePointConverter extends RichMapFunction<String, Point> {

		@Override
		public Point map(String str) {
			String[] fields = str.split("\\|");
			return new Point(Double.parseDouble(fields[1]), Double.parseDouble(fields[2]));
		}
	}

	/**
	 * Converts a {@code Tuple3<Integer, Double, Double>} into a {@link Centroid}.
	 */
	public static final class TupleCentroidConverter extends RichMapFunction<String, Centroid> {

		@Override
		public Centroid map(String str) {
			String[] fields = str.split("\\|");
			return new Centroid(Integer.parseInt(fields[0]), Double.parseDouble(fields[1]), Double.parseDouble(fields[2]));
		}
	}

	/**
	 * Determines the closest cluster center for a data point.
	 */
	public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {

		private Collection<Centroid> centroids;
		private CustomAccumulator acc;

		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
			this.acc = new CustomAccumulator();
			getRuntimeContext().addAccumulator("myAcc", this.acc);
		}

		@Override
		public Tuple2<Integer, Point> map(Point p) throws Exception {

			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;

			// check all cluster centers
			for (Centroid centroid : centroids) {
				// compute distance
				double distance = p.euclideanDistance(centroid);

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.id;
				}
			}

			// emit a new record with the center id and the data point.
			acc.add(1L);
			return new Tuple2<Integer, Point>(closestCentroidId, p);
		}
	}

	/**
	 * 	Use this so that we can check whether POJOs and the POJO comparator also work.
	 */
	public static final class DummyTuple3IntPointLong {
		public Integer field0;
		public Point field1;
		public Long field2;

		public DummyTuple3IntPointLong() {}

		DummyTuple3IntPointLong(Integer f0, Point f1, Long f2) {
			this.field0 = f0;
			this.field1 = f1;
			this.field2 = f2;
		}
	}

	/**
	 * Appends a count variable to the tuple.
	 */
	public static final class CountAppender extends RichMapFunction<Tuple2<Integer, Point>, DummyTuple3IntPointLong> {

		@Override
		public DummyTuple3IntPointLong map(Tuple2<Integer, Point> t) {
			return new DummyTuple3IntPointLong(t.f0, t.f1, 1L);
		}
	}

	/**
	 * Sums and counts point coordinates.
	 */
	public static final class CentroidAccumulator extends RichReduceFunction<DummyTuple3IntPointLong> {

		@Override
		public DummyTuple3IntPointLong reduce(DummyTuple3IntPointLong val1, DummyTuple3IntPointLong val2) {
			return new DummyTuple3IntPointLong(val1.field0, val1.field1.add(val2.field1), val1.field2 + val2.field2);
		}
	}

	/**
	 * Computes new centroid from coordinate sum and count of points.
	 */
	public static final class CentroidAverager extends RichMapFunction<DummyTuple3IntPointLong, Centroid> {

		@Override
		public Centroid map(DummyTuple3IntPointLong value) {
			return new Centroid(value.field0, value.field1.div(value.field2));
		}
	}

	private static class CustomAccumulator implements SimpleAccumulator<Long> {

		private long value;

		@Override
		public void add(Long value) {
			this.value += value;
		}

		@Override
		public Long getLocalValue() {
			return this.value;
		}

		@Override
		public void resetLocal() {
			this.value = 0L;
		}

		@Override
		public void merge(Accumulator<Long, Long> other) {
			this.value += other.getLocalValue();
		}

		@Override
		public Accumulator<Long, Long> clone() {
			CustomAccumulator acc = new CustomAccumulator();
			acc.value = this.value;
			return acc;
		}
	}
}
