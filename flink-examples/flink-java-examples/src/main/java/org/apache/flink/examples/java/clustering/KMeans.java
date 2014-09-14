/**
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

package org.apache.flink.examples.java.clustering;

import java.io.Serializable;
import java.util.Collection;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.clustering.util.KMeansData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * This example implements a basic K-Means clustering algorithm.
 * 
 * <p>
 * K-Means is an iterative clustering algorithm and works as follows:<br>
 * K-Means is given a set of data points to be clustered and an initial set of <i>K</i> cluster centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (<i>mean</i>) of all points that have been assigned to it.
 * The moved cluster centers are fed into the next iteration. 
 * The algorithm terminates after a fixed number of iterations (as in this implementation) 
 * or if cluster centers do not (significantly) move in an iteration.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/K-means_clustering">K-Means Clustering algorithm</a>.
 * 
 * <p>
 * This implementation works on two-dimensional data points. <br>
 * It computes an assignment of data points to cluster centers, i.e., 
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 * 
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character.
 * Data points are separated by newline characters.<br>
 * For example <code>"1.2 2.3\n5.3 7.2\n"</code> gives two data points (x=1.2, y=2.3) and (x=5.3, y=7.2).
 * <li>Cluster centers are represented by an integer id and a point value.<br>
 * For example <code>"1 6.2 3.2\n2 2.9 5.7\n"</code> gives two centers (id=1, x=6.2, y=3.2) and (id=2, x=2.9, y=5.7).
 * </ul>
 * 
 * <p>
 * Usage: <code>KMeans &lt;points path&gt; &lt;centers path&gt; &lt;result path&gt; &lt;num iterations&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link KMeansData} and 10 iterations. 
 * 
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Bulk iterations
 * <li>Broadcast variables in bulk iterations
 * <li>Custom Java objects (PoJos)
 * </ul>
 */
@SuppressWarnings("serial")
public class KMeans {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {
		
		if(!parseParameters(args)) {
			return;
		}
	
		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// get input data
		DataSet<Point> points = getPointDataSet(env);
		DataSet<Centroid> centroids = getCentroidDataSet(env);
		
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
		if(fileOutput) {
			clusteredPoints.writeAsCsv(outputPath, "\n", " ");
		} else {
			clusteredPoints.print();
		}

		// execute program
		env.execute("KMeans Example");
		
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
	public static final class TuplePointConverter implements MapFunction<Tuple2<Double, Double>, Point> {

		@Override
		public Point map(Tuple2<Double, Double> t) throws Exception {
			return new Point(t.f0, t.f1);
		}
	}
	
	/** Converts a Tuple3<Integer, Double,Double> into a Centroid. */
	public static final class TupleCentroidConverter implements MapFunction<Tuple3<Integer, Double, Double>, Centroid> {

		@Override
		public Centroid map(Tuple3<Integer, Double, Double> t) throws Exception {
			return new Centroid(t.f0, t.f1, t.f2);
		}
	}
	
	/** Determines the closest cluster center for a data point. */
	public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
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
	public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
			return new Tuple3<Integer, Point, Long>(t.f0, t.f1, 1L);
		} 
	}
	
	/** Sums and counts point coordinates. */
	public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
			return new Tuple3<Integer, Point, Long>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
		}
	}
	
	/** Computes new centroid from coordinate sum and count of points. */
	public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

		@Override
		public Centroid map(Tuple3<Integer, Point, Long> value) {
			return new Centroid(value.f0, value.f1.div(value.f2));
		}
	}
	
	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	
	private static boolean fileOutput = false;
	private static String pointsPath = null;
	private static String centersPath = null;
	private static String outputPath = null;
	private static int numIterations = 10;
	
	private static boolean parseParameters(String[] programArguments) {
		
		if(programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(programArguments.length == 4) {
				pointsPath = programArguments[0];
				centersPath = programArguments[1];
				outputPath = programArguments[2];
				numIterations = Integer.parseInt(programArguments[3]);
			} else {
				System.err.println("Usage: KMeans <points path> <centers path> <result path> <num iterations>");
				return false;
			}
		} else {
			System.out.println("Executing K-Means example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  We provide a data generator to create synthetic input files for this program.");
			System.out.println("  Usage: KMeans <points path> <centers path> <result path> <num iterations>");
		}
		return true;
	}
	
	private static DataSet<Point> getPointDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			// read points from CSV file
			return env.readCsvFile(pointsPath)
						.fieldDelimiter(' ')
						.includeFields(true, true)
						.types(Double.class, Double.class)
						.map(new TuplePointConverter());
		} else {
			return KMeansData.getDefaultPointDataSet(env);
		}
	}
	
	private static DataSet<Centroid> getCentroidDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			return env.readCsvFile(centersPath)
						.fieldDelimiter(' ')
						.includeFields(true, true, true)
						.types(Integer.class, Double.class, Double.class)
						.map(new TupleCentroidConverter());
		} else {
			return KMeansData.getDefaultCentroidDataSet(env);
		}
	}
		
}
