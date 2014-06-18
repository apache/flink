/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.test.util.testjar;

import java.io.Serializable;
import java.util.Collection;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.configuration.Configuration;

@SuppressWarnings("serial")
public class KMeansForTest implements Program {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	

	@Override
	public Plan getPlan(String... args) {
		if (args.length < 4) {
			throw new IllegalArgumentException("Missing parameters");
		}
		
		final String pointsPath = args[0];
		final String centersPath = args[1];
		final String outputPath = args[2];
		final int numIterations = Integer.parseInt(args[3]);
		
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(4);
		
		// get input data
		DataSet<Point> points = env.readCsvFile(pointsPath)
				.fieldDelimiter('|')
				.includeFields(true, true)
				.types(Double.class, Double.class)
				.map(new TuplePointConverter());
		
		DataSet<Centroid> centroids = env.readCsvFile(centersPath)
				.fieldDelimiter('|')
				.includeFields(true, true, true)
				.types(Integer.class, Double.class, Double.class)
				.map(new TupleCentroidConverter());
		
		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Centroid> loop = centroids.iterate(numIterations);
		
		DataSet<Centroid> newCentroids = points
			// compute closest centroid for each point
			.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
			// count and sum point coordinates for each centroid
			.map(new CountAppender())
			.groupBy(0).reduce(new CentroidAccumulator())
			// compute new centroids from point counts and coordinate sums
			.map(new CentroidAverager());
		
		// feed new centroids back into next iteration
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);
		
		DataSet<Tuple2<Integer, Point>> clusteredPoints = points
				// assign points to final clusters
				.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");
		
		// emit result
		clusteredPoints.writeAsCsv(outputPath, "\n", " ");

		return env.createProgramPlan();
	}
	
	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************
	
	/**
	 * A simple two-dimensional point.
	 */
	public static class Point implements Serializable {
		
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
			return Math.sqrt((x-other.x)*(x-other.x) + (y-other.y)*(y-other.y));
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
			super(x,y);
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
	
	/** Converts a Tuple2<Double,Double> into a Point. */
	public static final class TuplePointConverter extends MapFunction<Tuple2<Double, Double>, Point> {

		@Override
		public Point map(Tuple2<Double, Double> t) throws Exception {
			return new Point(t.f0, t.f1);
		}
	}
	
	/** Converts a Tuple3<Integer, Double,Double> into a Centroid. */
	public static final class TupleCentroidConverter extends MapFunction<Tuple3<Integer, Double, Double>, Centroid> {

		@Override
		public Centroid map(Tuple3<Integer, Double, Double> t) throws Exception {
			return new Centroid(t.f0, t.f1, t.f2);
		}
	}
	
	/** Determines the closest cluster center for a data point. */
	public static final class SelectNearestCenter extends MapFunction<Point, Tuple2<Integer, Point>> {
		private Collection<Centroid> centroids;

		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
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
			return new Tuple2<Integer, Point>(closestCentroidId, p);
		}
	}
	
	/** Appends a count variable to the tuple. */ 
	public static final class CountAppender extends MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
			return new Tuple3<Integer, Point, Long>(t.f0, t.f1, 1L);
		} 
	}
	
	/** Sums and counts point coordinates. */
	public static final class CentroidAccumulator extends ReduceFunction<Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
			return new Tuple3<Integer, Point, Long>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
		}
	}
	
	/** Computes new centroid from coordinate sum and count of points. */
	public static final class CentroidAverager extends MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

		@Override
		public Centroid map(Tuple3<Integer, Point, Long> value) {
			return new Centroid(value.f0, value.f1.div(value.f2));
		}
	}
}
