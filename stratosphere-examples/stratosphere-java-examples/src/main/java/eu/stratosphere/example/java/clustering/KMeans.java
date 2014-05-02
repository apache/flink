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
package eu.stratosphere.example.java.clustering;

import java.io.Serializable;
import java.util.Collection;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.configuration.Configuration;


@SuppressWarnings("serial")
public class KMeans {
	
	/**
	 * A simple three-dimensional point.
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
			return "(" + x + "|" + y + ")";
		}
	}
	
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
	}
	
	/**
	 * Determines the closest cluster center for a data point.
	 */
	public static final class SelectNearestCenter extends MapFunction<Point, Tuple3<Integer, Point, Long>> {
		private static final long serialVersionUID = 1L;

		private Collection<Centroid> centroids;

		/** Reads the centroid values from the broadcast variable into a collection.*/
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple3<Integer, Point, Long> map(Point p) throws Exception {
			
			double nearestDistance = Double.MAX_VALUE;
			int centroidId = 0;
			
			// check all cluster centers
			for (Centroid centroid : centroids) {
				// compute distance
				double distance = p.euclideanDistance(centroid);
				
				// update nearest cluster if necessary 
				if (distance < nearestDistance) {
					nearestDistance = distance;
					centroidId = centroid.id;
				}
			}

			// emit a new record with the center id and the data point.
			return new Tuple3<Integer, Point, Long>(centroidId, p, 1L);
		}
	}
	
	
	/** The input and output types are (centroid-id, point-sum, count) */
	public static class CentroidAccumulator extends ReduceFunction<Tuple3<Integer, Point, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
			return new Tuple3<Integer, Point, Long>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
		}
	}

	
	/** The input and output types are (centroid-id, point-sum, count) */
	public static class CentroidAverager extends MapFunction<Tuple3<Integer, Point, Long>, Centroid> {
		private static final long serialVersionUID = 1L;

		@Override
		public Centroid map(Tuple3<Integer, Point, Long> value) {
			return new Centroid(value.f0, value.f1.div(value.f2));
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Point> points = env.fromElements(new Point(-3.78, -42.01), new Point(-45.96, 30.67), new Point(8.96, -41.58),
												new Point(-22.96, 40.73), new Point(4.79, -35.58), new Point(-41.27, 32.42),
												new Point(-2.61, -30.43), new Point(-23.33, 26.23), new Point(-9.22, -31.23),
												new Point(-45.37, 36.42));
		
		DataSet<Centroid> centroids = env.fromElements(new Centroid(0, 43.28, 47.89),
													new Centroid(1, -0.06, -48.97));
		
		IterativeDataSet<Centroid> loop = centroids.iterate(20);
		
		DataSet<Centroid> newCentriods = points
			.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
			.groupBy(0).reduce(new CentroidAccumulator())
			.map(new CentroidAverager());
		
		DataSet<Centroid> result = loop.closeWith(newCentriods);
		
		result.print();
		
		env.execute("KMeans 2d example");
	}
}
