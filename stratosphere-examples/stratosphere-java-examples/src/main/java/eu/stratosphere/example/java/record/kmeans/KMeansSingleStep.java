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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.java.record.kmeans.udfs.PointInFormat;
import eu.stratosphere.example.java.record.kmeans.udfs.PointOutFormat;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;


public class KMeansSingleStep implements Program, ProgramDescription {
	

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataPointInput = (args.length > 1 ? args[1] : "");
		String clusterInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for data point input
		FileDataSource dataPoints = new FileDataSource(new PointInFormat(), dataPointInput, "Data Points");

		// create DataSourceContract for cluster center input
		FileDataSource clusterPoints = new FileDataSource(new PointInFormat(), clusterInput, "Centers");

		// create CrossOperator for distance computation
		MapOperator findNearestClusterCenters = MapOperator.builder(new SelectNearestCenter())
			.setBroadcastVariable("centers", clusterPoints)
			.input(dataPoints)
			.name("Find Nearest Centers")
			.build();

		// create ReduceOperator for computing new cluster positions
		ReduceOperator recomputeClusterCenter = ReduceOperator.builder(new RecomputeClusterCenter(), IntValue.class, 0)
			.input(findNearestClusterCenters)
			.name("Recompute Center Positions")
			.build();

		// create DataSinkContract for writing the new cluster positions
		FileDataSink newClusterPoints = new FileDataSink(new PointOutFormat(), output, recomputeClusterCenter, "New Center Positions");

		// return the PACT plan
		Plan plan = new Plan(newClusterPoints, "KMeans Iteration");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: <numSubStasks> <dataPoints> <clusterCenters> <output>";
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
		public void write(DataOutput out) throws IOException {
			out.writeDouble(x);
			out.writeDouble(y);
			out.writeDouble(z);
		}

		@Override
		public void read(DataInput in) throws IOException {
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
	public static final class SelectNearestCenter extends MapFunction implements Serializable {
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
			Point p = dataPointRecord.getField(0, Point.class);
			
			double nearestDistance = Double.MAX_VALUE;
			int centerId = -1;

			// check all cluster centers
			for (PointWithId center : centers) {
				// compute distance
				double distance = p.euclideanDistance(center.point);
				
				// update nearest cluster if necessary 
				if (distance < nearestDistance) {
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
	
	public static final class RecomputeClusterCenter extends ReduceFunction implements Serializable {
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
}
