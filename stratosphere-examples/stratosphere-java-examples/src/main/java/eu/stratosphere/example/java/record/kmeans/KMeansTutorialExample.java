/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.example.java.record.kmeans;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.FileOutputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;


public class KMeansTutorialExample implements Program, ProgramDescription {
	
	public static final String CENTERS_RESULT_SUBDIRECTORY = "centers";
	public static final String POINTS_RESULT_SUBDIRECTORY = "points";
	
	private static final long serialVersionUID = 1L;

	@Override
	@SuppressWarnings("unchecked")
	public Plan getPlan(String... args) {
		
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String dataPointInput = (args.length > 1 ? args[1] : "");
		final String clusterInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int numIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 2);
		
		final String centersOutput = output + "/" + CENTERS_RESULT_SUBDIRECTORY;
		final String taggedPointsOutput = output + "/" + POINTS_RESULT_SUBDIRECTORY;

		// create DataSourceContract for data point input
		FileDataSource pointsSource = new FileDataSource(new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class), dataPointInput, "Data Points");

		// create DataSourceContract for cluster center input
		FileDataSource clustersSource = new FileDataSource(new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class), clusterInput, "Centers");
		
		MapOperator dataPoints = MapOperator.builder(new PointBuilder()).name("Build data points").input(pointsSource).build();
		
		MapOperator clusterPoints = MapOperator.builder(new PointBuilder()).name("Build cluster points").input(clustersSource).build();
		
		// ---------------------- Begin K-Means Loop ---------------------
		
		BulkIteration iter = new BulkIteration("k-means loop");
		iter.setInput(clusterPoints);
		iter.setMaximumNumberOfIterations(numIterations);

		// compute the distances and select the closest center
		MapOperator findNearestClusterCenters = MapOperator.builder(new SelectNearestCenter())
			.setBroadcastVariable("centers", iter.getPartialSolution())
			.input(dataPoints)
			.name("Find Nearest Centers")
			.build();

		//  computing the new cluster positions
		ReduceOperator recomputeClusterCenter = ReduceOperator.builder(new RecomputeClusterCenter(), IntValue.class, 0)
			.input(findNearestClusterCenters)
			.name("Recompute Center Positions")
			.build();
		
		iter.setNextPartialSolution(recomputeClusterCenter);
		
		// ---------------------- End K-Means Loop ---------------------
		
		
		// run one additional points-to-centers assignment, to have the points tagged with the final centers they belong to
		// create DataSourceContract for data point input
		
		MapOperator findNearestFinalCluster = MapOperator.builder(new SelectNearestCenter())
			.setBroadcastVariable("centers", iter)
			.input(dataPoints)
			.name("Tag points with final centroid")
			.build();

		// write the new cluster positions
		FileDataSink newClusterPoints = new FileDataSink(new PointOutFormat(), centersOutput, iter, "Cluster Positions");

		// write assigned clusters
		FileDataSink clusterAssignments = new FileDataSink(new PointOutFormat(), taggedPointsOutput, findNearestFinalCluster, "Cluster Assignments");

		// return the plan
		Plan plan = new Plan(newClusterPoints, "KMeans Iteration");
		plan.addDataSink(clusterAssignments);
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>";
	}
	
	// --------------------------------------------------------------------------------------------
	//  Data Types and UDFs
	// --------------------------------------------------------------------------------------------
	
	/**
	 * A simple two-dimensional point.
	 */
	public static final class Point implements Value {
		private static final long serialVersionUID = 1L;
		
		public double x, y;
		
		public Point() {}

		public Point(double x, double y) {
			this.x = x;
			this.y = y;
		}
		
		public void add(Point other) {
			x += other.x;
			y += other.y;
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
		public void write(DataOutput out) throws IOException {
			out.writeDouble(x);
			out.writeDouble(y);
		}

		@Override
		public void read(DataInput in) throws IOException {
			x = in.readDouble();
			y = in.readDouble();
		}
		
		@Override
		public String toString() {
			return "(" + x + "|" + y + ")";
		}
	}
	
	public static final class PointWithId {
		
		public int id;
		public Point point;
		
		public PointWithId(int id, Point p) {
			this.id = id;
			this.point = p;
		}
	}
	
	/**
	 * Determines the closest cluster center for a data point.
	 */
	public static final class SelectNearestCenter extends MapFunction {
		private static final long serialVersionUID = 1L;

		private final IntValue one = new IntValue(1);
		private final Record result = new Record(3);

		private List<PointWithId> centers = new ArrayList<PointWithId>();

		/**
		 * Reads all the center values from the broadcast variable into a collection.
		 */
		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<Record> clusterCenters = this.getRuntimeContext().getBroadcastVariable("centers");
			
			centers.clear();
			for (Record r : clusterCenters) {
				centers.add(new PointWithId(r.getField(0, IntValue.class).getValue(), r.getField(1, Point.class)));
			}
		}

		/**
		 * Computes a minimum aggregation on the distance of a data point to cluster centers.
		 * 
		 * Output Format:
		 * 0: centerID
		 * 1: pointVector
		 * 2: constant(1) (to enable combinable average computation in the following reducer)
		 */
		@Override
		public void map(Record dataPointRecord, Collector<Record> out) {
			Point p = dataPointRecord.getField(1, Point.class);
			
			double nearestDistance = Double.MAX_VALUE;
			int centerId = -1;

			// check all cluster centers
			for (PointWithId center : centers) {
				// compute distance
				double distance = p.euclideanDistance(center.point);
				
				// update nearest cluster if necessary 
				if (distance < nearestDistance) {
					nearestDistance = distance;
					centerId = center.id;
				}
			}

			// emit a new record with the center id and the data point. add a one to ease the
			// implementation of the average function with a combiner
			result.setField(0, new IntValue(centerId));
			result.setField(1, p);
			result.setField(2, one);

			out.collect(result);
		}
	}
	
	@Combinable
	public static final class RecomputeClusterCenter extends ReduceFunction {
		private static final long serialVersionUID = 1L;
		
		private final Point p = new Point();
		
		
		/**
		 * Compute the new position (coordinate vector) of a cluster center.
		 */
		@Override
		public void reduce(Iterator<Record> points, Collector<Record> out) {
			Record sum = sumPointsAndCount(points);
			sum.setField(1, sum.getField(1, Point.class).div(sum.getField(2, IntValue.class).getValue()));
			out.collect(sum);
		}

		/**
		 * Computes a pre-aggregated average value of a coordinate vector.
		 */
		@Override
		public void combine(Iterator<Record> points, Collector<Record> out) {
			out.collect(sumPointsAndCount(points));
		}
		
		private final Record sumPointsAndCount(Iterator<Record> dataPoints) {
			Record next = null;
			p.clear();
			int count = 0;
			
			// compute coordinate vector sum and count
			while (dataPoints.hasNext()) {
				next = dataPoints.next();
				p.add(next.getField(1, Point.class));
				count += next.getField(2, IntValue.class).getValue();
			}
			
			next.setField(1, p);
			next.setField(2, new IntValue(count));
			return next;
		}
	}
	
	public static final class PointBuilder extends MapFunction {

		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			double x = record.getField(1, DoubleValue.class).getValue();
			double y = record.getField(2, DoubleValue.class).getValue();
			
			record.setField(1, new Point(x, y));
			out.collect(record);
		}
	}
	
	public static final class PointOutFormat extends FileOutputFormat {

		private static final long serialVersionUID = 1L;
		
		private static final String format = "%d|%.2f|%.2f|\n";

		@Override
		public void writeRecord(Record record) throws IOException {
			int id = record.getField(0, IntValue.class).getValue();
			Point p = record.getField(1, Point.class);
			
			byte[] bytes = String.format(format, id, p.x, p.y).getBytes();
			
			this.stream.write(bytes);
		}
	}
}
