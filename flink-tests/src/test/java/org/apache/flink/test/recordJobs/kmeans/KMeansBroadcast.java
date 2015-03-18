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

package org.apache.flink.test.recordJobs.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.api.java.record.operators.BulkIteration;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator.Combinable;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public class KMeansBroadcast implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int parallelism = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataPointInput = (args.length > 1 ? args[1] : "");
		String clusterInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");
		int numIterations = (args.length > 4 ? Integer.parseInt(args[4]) : 2);

		// data source data point input
		@SuppressWarnings("unchecked")
		FileDataSource pointsSource = new FileDataSource(new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class, DoubleValue.class), dataPointInput, "Data Points");

		// data source for cluster center input
		@SuppressWarnings("unchecked")
		FileDataSource clustersSource = new FileDataSource(new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class, DoubleValue.class), clusterInput, "Centers");
		
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

		// computing the new cluster positions
		ReduceOperator recomputeClusterCenter = ReduceOperator.builder(new RecomputeClusterCenter(), IntValue.class, 0)
			.input(findNearestClusterCenters)
			.name("Recompute Center Positions")
			.build();
		
		iter.setNextPartialSolution(recomputeClusterCenter);

		// ---------------------- End K-Means Loop ---------------------
		
		// create DataSinkContract for writing the new cluster positions
		FileDataSink newClusterPoints = new FileDataSink(new PointOutFormat(), output, iter, "New Center Positions");

		Plan plan = new Plan(newClusterPoints, "K-Means");
		plan.setDefaultParallelism(parallelism);
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
	 * A simple three-dimensional point.
	 */
	public static final class Point implements Value {
		private static final long serialVersionUID = 1L;
		
		public double x, y, z;
		
		public Point() {}

		public Point(double x, double y, double z) {
			this.x = x;
			this.y = y;
			this.z = z;
		}
		
		public void add(Point other) {
			x += other.x;
			y += other.y;
			z += other.z;
		}
		
		public Point div(long val) {
			x /= val;
			y /= val;
			z /= val;
			return this;
		}
		
		public double euclideanDistance(Point other) {
			return Math.sqrt((x-other.x)*(x-other.x) + (y-other.y)*(y-other.y) + (z-other.z)*(z-other.z));
		}
		
		public void clear() {
			x = y = z = 0.0;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeDouble(x);
			out.writeDouble(y);
			out.writeDouble(z);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			x = in.readDouble();
			y = in.readDouble();
			z = in.readDouble();
		}
		
		@Override
		public String toString() {
			return "(" + x + "|" + y + "|" + z + ")";
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
			List<Record> clusterCenters = this.getRuntimeContext().getBroadcastVariable("centers");
			
			centers.clear();
			synchronized (clusterCenters) {
				for (Record r : clusterCenters) {
					centers.add(new PointWithId(r.getField(0, IntValue.class).getValue(), r.getField(1, Point.class)));
				}
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
			double z = record.getField(3, DoubleValue.class).getValue();
			
			record.setField(1, new Point(x, y, z));
			out.collect(record);
		}
	}
	
	public static final class PointOutFormat extends FileOutputFormat {

		private static final long serialVersionUID = 1L;
		
		private static final String format = "%d|%.1f|%.1f|%.1f|\n";

		@Override
		public void writeRecord(Record record) throws IOException {
			int id = record.getField(0, IntValue.class).getValue();
			Point p = record.getField(1, Point.class);
			
			byte[] bytes = String.format(format, id, p.x, p.y, p.z).getBytes();
			
			this.stream.write(bytes);
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(LocalExecutor.optimizerPlanAsJSON(new KMeansBroadcast().getPlan("4", "/dev/random", "/dev/random", "/tmp", "20")));
	}
}
