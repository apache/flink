/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.example.datamining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.AddSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSetFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ReadSetSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.UpdateSet;
import eu.stratosphere.pact.common.stubs.StubAnnotation.UpdateSetFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.UpdateSetSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.UpdateSet.UpdateSetMode;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * The K-Means cluster algorithm is well-known (see
 * http://en.wikipedia.org/wiki/K-means_clustering). KMeansIteration is a PACT
 * program that computes a single iteration of the k-means algorithm. The job
 * has two inputs, a set of data points and a set of cluster centers. A Cross
 * PACT is used to compute all distances from all centers to all points. A
 * following Reduce PACT assigns each data point to the cluster center that is
 * next to it. Finally, a second Reduce PACT compute the new locations of all
 * cluster centers.
 * 
 * @author Fabian Hueske
 */
public class KMeansIteration implements PlanAssembler, PlanAssemblerDescription
{
	/**
	 * Implements a feature vector as a multi-dimensional point. Coordinates of that point
	 * (= the features) are stored as double values. The distance between two feature vectors is
	 * the Euclidian distance between the points.
	 * 
	 * @author Fabian Hueske
	 */
	public static final class CoordVector implements Key
	{
		// coordinate array
		private double[] coordinates;

		/**
		 * Initializes a blank coordinate vector. Required for deserialization!
		 */
		public CoordVector() {
			coordinates = null;
		}

		/**
		 * Initializes a coordinate vector.
		 * 
		 * @param coordinates The coordinate vector of a multi-dimensional point.
		 */
		public CoordVector(Double[] coordinates)
		{
			this.coordinates = new double[coordinates.length];
			for (int i = 0; i < coordinates.length; i++) {
				this.coordinates[i] = coordinates[i];
			}
		}

		/**
		 * Initializes a coordinate vector.
		 * 
		 * @param coordinates The coordinate vector of a multi-dimensional point.
		 */
		public CoordVector(double[] coordinates) {
			this.coordinates = coordinates;
		}

		/**
		 * Returns the coordinate vector of a multi-dimensional point.
		 * 
		 * @return The coordinate vector of a multi-dimensional point.
		 */
		public double[] getCoordinates() {
			return this.coordinates;
		}
		
		/**
		 * Sets the coordinate vector of a multi-dimensional point.
		 * 
		 * @param point The dimension values of the point.
		 */
		public void setCoordinates(double[] coordinates) {
			this.coordinates = coordinates;
		}

		/**
		 * Computes the Euclidian distance between this coordinate vector and a
		 * second coordinate vector.
		 * 
		 * @param cv The coordinate vector to which the distance is computed.
		 * @return The Euclidian distance to coordinate vector cv. If cv has a
		 *         different length than this coordinate vector, -1 is returned.
		 */
		public double computeEuclidianDistance(CoordVector cv)
		{
			// check coordinate vector lengths
			if (cv.coordinates.length != this.coordinates.length) {
				return -1.0;
			}

			double quadSum = 0.0;
			for (int i = 0; i < this.coordinates.length; i++) {
				double diff = this.coordinates[i] - cv.coordinates[i];
				quadSum += diff*diff;
			}
			return Math.sqrt(quadSum);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void read(DataInput in) throws IOException {
			int length = in.readInt();
			this.coordinates = new double[length];
			for (int i = 0; i < length; i++) {
				this.coordinates[i] = in.readDouble();
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(this.coordinates.length);
			for (int i = 0; i < this.coordinates.length; i++) {
				out.writeDouble(this.coordinates[i]);
			}
		}

		/**
		 * Compares this coordinate vector to another key.
		 * 
		 * @return -1 if the other key is not of type CoordVector. If the other
		 *         key is also a CoordVector but its length differs from this
		 *         coordinates vector, -1 is return if this coordinate vector is
		 *         smaller and 1 if it is larger. If both coordinate vectors
		 *         have the same length, the coordinates of both are compared.
		 *         If a coordinate of this coordinate vector is smaller than the
		 *         corresponding coordinate of the other vector -1 is returned
		 *         and 1 otherwise. If all coordinates are identical 0 is
		 *         returned.
		 */
		@Override
		public int compareTo(Key o)
		{
			// check if other key is also of type CoordVector
			if (!(o instanceof CoordVector)) {
				return -1;
			}
			// cast to CoordVector
			CoordVector oP = (CoordVector) o;

			// check if both coordinate vectors have identical lengths
			if (oP.coordinates.length > this.coordinates.length) {
				return -1;
			}
			else if (oP.coordinates.length < this.coordinates.length) {
				return 1;
			}

			// compare all coordinates
			for (int i = 0; i < this.coordinates.length; i++) {
				if (oP.coordinates[i] > this.coordinates[i]) {
					return -1;
				} else if (oP.coordinates[i] < this.coordinates[i]) {
					return 1;
				}
			}
			return 0;
		}
	}

	/**
	 * Reads key-value pairs with N_Integer as key type and CoordVector as value
	 * type. The input format is line-based, i.e. one pair is written to a line
	 * and terminated with '\n'. Within a line the first '|' character separates
	 * the key from the value. The value consists of a vector of decimals. The
	 * decimals are separated by '|'. The key is the id of a data point or
	 * cluster center and the value the corresponding position (coordinate
	 * vector) of the data point or cluster center. Example line:
	 * "42|23.23|52.57|74.43| Id: 42 Coordinate vector: (23.23, 52.57, 74.43)
	 * 
	 * @author Fabian Hueske
	 */
	public static class PointInFormat extends DelimitedInputFormat
	{
		private final PactInteger idInteger = new PactInteger();
		private final CoordVector point = new CoordVector();
		
		private final List<Double> dimensionValues = new ArrayList<Double>();
		private double[] pointValues = new double[0];
		
		@Override
		public boolean readRecord(PactRecord record, byte[] line, int numBytes)
		{
			int id = -1;
			int value = 0;
			int fractionValue = 0;
			int fractionChars = 0;
			
			this.dimensionValues.clear();

			for (int pos = 0; pos < numBytes; pos++) {
				if (line[pos] == '|') {
					// check if id was already set
					if (id == -1) {
						id = value;
					}
					else {
						this.dimensionValues.add(value + ((double) fractionValue) * Math.pow(10, (-1 * (fractionChars - 1))));
					}
					// reset value
					value = 0;
					fractionValue = 0;
					fractionChars = 0;
				} else if (line[pos] == '.') {
					fractionChars = 1;
				} else {
					if (fractionChars == 0) {
						value *= 10;
						value += line[pos] - '0';
					} else {
						fractionValue *= 10;
						fractionValue += line[pos] - '0';
						fractionChars++;
					}
				}
			}

			// set the ID
			this.idInteger.setValue(id);
			record.setField(0, this.idInteger);
			
			// set the data points
			if (this.pointValues.length != this.dimensionValues.size()) {
				this.pointValues = new double[this.dimensionValues.size()];
			}
			for (int i = 0; i < this.pointValues.length; i++) {
				this.pointValues[i] = this.dimensionValues.get(i);
			}
			
			this.point.setCoordinates(this.pointValues);
			record.setField(1, this.point);
			return true;
		}
	}

	/**
	 * Writes key-value pairs with N_Integer as key type and CoordVector as
	 * value type. The output format is line-based, i.e. one pair is written to
	 * a line and terminated with '\n'. Within a line the first '|' character
	 * separates the key from the value. The value consists of a vector of
	 * decimals. The decimals are separated by '|'. The key is the id of a data
	 * point or cluster center and the value the corresponding position
	 * (coordinate vector) of the data point or cluster center. Example line:
	 * "42|23.23|52.57|74.43| Id: 42 Coordinate vector: (23.23, 52.57, 74.43)
	 * 
	 * @author Fabian Hueske
	 */
	public static class PointOutFormat extends DelimitedOutputFormat
	{
		private final DecimalFormat df = new DecimalFormat("####0.00");
		private final StringBuilder line = new StringBuilder();
		
		
		public PointOutFormat() {
			DecimalFormatSymbols dfSymbols = new DecimalFormatSymbols();
			dfSymbols.setDecimalSeparator('.');
			this.df.setDecimalFormatSymbols(dfSymbols);
		}
		
		@Override
		public int serializeRecord(PactRecord record, byte[] target)
		{
			line.setLength(0);
			
			PactInteger centerId = record.getField(0, PactInteger.class);
			CoordVector centerPos = record.getField(1, CoordVector.class);
			
			
			line.append(centerId.getValue());

			for (double coord : centerPos.getCoordinates()) {
				line.append('|');
				line.append(df.format(coord));
			}
			line.append('|');
			
			byte[] byteString = line.toString().getBytes();
			
			if (byteString.length <= target.length) {
				System.arraycopy(byteString, 0, target, 0, byteString.length);
				return byteString.length;
			}
			else {
				return -byteString.length;
			}
		}
	}

	/**
	 * Cross PACT computes the distance of all data points to all cluster
	 * centers.
	 * <p>
	 * 
	 * @author Fabian Hueske
	 */
	@ReadSetFirst(fields={1})
	@UpdateSetFirst(fields={})
	@ReadSetSecond(fields={1})
	@UpdateSetSecond(fields={}, setMode=UpdateSetMode.Constant)
	@AddSet(fields={2,3})
	@OutCardBounds(lowerBound=1, upperBound=1)
	public static class ComputeDistance extends	CrossStub
	{
		private final PactDouble distance = new PactDouble();
		
		/**
		 * Computes the distance of one data point to one cluster center and
		 * emits a key-value-pair where the id of the data point is the key and
		 * a Distance object is the value.
		 */
		@Override
		public void cross(PactRecord dataPointRecord, PactRecord clusterCenterRecord, Collector out)
		{
			CoordVector dataPoint = dataPointRecord.getField(1, CoordVector.class);
			
			PactInteger clusterCenterId = clusterCenterRecord.getField(0, PactInteger.class);
			CoordVector clusterPoint = clusterCenterRecord.getField(1, CoordVector.class);
		
			this.distance.setValue(dataPoint.computeEuclidianDistance(clusterPoint));
			
			// add cluster center id and distance to the data point record 
			dataPointRecord.setField(2, clusterCenterId);
			dataPointRecord.setField(3, this.distance);
			
			out.collect(dataPointRecord);
		}
	}

	/**
	 * Reduce PACT determines the closes cluster center for a data point. This
	 * is a minimum aggregation. Hence, a Combiner can be easily implemented.
	 * 
	 * @author Fabian Hueske
	 */
	@ReadSet(fields={3})
	@UpdateSet(fields={}, setMode=UpdateSetMode.Constant)
	@AddSet(fields={})
	@OutCardBounds(lowerBound=1, upperBound=1)
	@Combinable
	public static class FindNearestCenter extends ReduceStub
	{
		private final PactInteger centerId = new PactInteger();
		private final CoordVector position = new CoordVector();
		private final PactInteger one = new PactInteger(1);
		
		private final PactRecord result = new PactRecord(3);
		
		/**
		 * Computes a minimum aggregation on the distance of a data point to
		 * cluster centers. Emits a key-value-pair where the key is the id of
		 * the closest cluster center and the value is the coordinate vector of
		 * the data point. The CoordVectorCountSum data type is used to enable
		 * the use of a Combiner for the second Reduce PACT.
		 */
		@Override
		public void reduce(Iterator<PactRecord> pointsWithDistance, Collector out)
		{
			double nearestDistance = Double.MAX_VALUE;
			int nearestClusterId = 0;

			// check all cluster centers
			while (pointsWithDistance.hasNext())
			{
				PactRecord res = pointsWithDistance.next();
				
				double distance = res.getField(3, PactDouble.class).getValue();
				int currentId = res.getField(2, PactInteger.class).getValue();

				// compare distances
				if (distance < nearestDistance) {
					// if distance is smaller than smallest till now, update nearest cluster
					nearestDistance = distance;
					nearestClusterId = currentId;
					res.getFieldInto(1, this.position);
				}
			}

			// emit a new record with the center id and the data point. add a one to ease the
			// implementation of the average function with a combiner
			this.centerId.setValue(nearestClusterId);
			this.result.setField(0, this.centerId);
			this.result.setField(1, this.position);
			this.result.setField(2, this.one);
				
			out.collect(this.result);
		}

		// ----------------------------------------------------------------------------------------
		
		private final PactRecord nearest = new PactRecord();
		/**
		 * Computes a minimum aggregation on the distance of a data point to
		 * cluster centers.
		 */
		@Override
		public void combine(Iterator<PactRecord> pointsWithDistance, Collector out)
		{	
			double nearestDistance = Double.MAX_VALUE;

			// check all cluster centers
			while (pointsWithDistance.hasNext())
			{
				PactRecord res = pointsWithDistance.next();
				double distance = res.getField(3, PactDouble.class).getValue();

				// compare distances
				if (distance < nearestDistance) {
					nearestDistance = distance;
					res.copyTo(this.nearest);
				}
			}

			// emit nearest one
			out.collect(this.nearest);
		}
	}

	/**
	 * Reduce PACT computes the new position (coordinate vector) of a cluster
	 * center. This is an average computation. Hence, Combinable is annotated
	 * and the combine method implemented. SameKey is annotated because the
	 * PACT's output key is identical to its input key.
	 * 
	 * @author Fabian Hueske
	 */
	
	@ReadSet(fields={1,2})
	@UpdateSet(fields={1,2}, setMode=UpdateSetMode.Update)
	@AddSet(fields={})
	@OutCardBounds(lowerBound=1, upperBound=1)
	@Combinable
	public static class RecomputeClusterCenter extends ReduceStub
	{
		private final PactInteger count = new PactInteger();
		
		/**
		 * Compute the new position (coordinate vector) of a cluster center.
		 */
		@Override
		public void reduce(Iterator<PactRecord> dataPoints, Collector out)
		{
			PactRecord next = null;
				
			// initialize coordinate vector sum and count
			CoordVector coordinates = new CoordVector();
			double[] coordinateSum = null;
			int count = 0;	

			// compute coordinate vector sum and count
			while (dataPoints.hasNext())
			{
				next = dataPoints.next();
				
				// get the coordinates and the count from the record
				double[] thisCoords = next.getField(1, CoordVector.class).getCoordinates();
				int thisCount = next.getField(2, PactInteger.class).getValue();
				
				if (coordinateSum == null) {
					if (coordinates.getCoordinates() != null) {
						coordinateSum = coordinates.getCoordinates();
					}
					else {
						coordinateSum = new double[thisCoords.length];
					}
				}

				addToCoordVector(coordinateSum, thisCoords);
				count += thisCount;
			}

			// compute new coordinate vector (position) of cluster center
			for (int i = 0; i < coordinateSum.length; i++) {
				coordinateSum[i] /= count;
			}
			
			coordinates.setCoordinates(coordinateSum);
			next.setField(1, coordinates);

			// emit new position of cluster center
			out.collect(next);
		}

		/**
		 * Computes a pre-aggregated average value of a coordinate vector.
		 */
		@Override
		public void combine(Iterator<PactRecord> dataPoints, Collector out)
		{
			PactRecord next = null;
			
			// initialize coordinate vector sum and count
			CoordVector coordinates = new CoordVector();
			double[] coordinateSum = null;
			int count = 0;	

			// compute coordinate vector sum and count
			while (dataPoints.hasNext())
			{
				next = dataPoints.next();
				
				// get the coordinates and the count from the record
				double[] thisCoords = next.getField(1, CoordVector.class).getCoordinates();
				int thisCount = next.getField(2, PactInteger.class).getValue();
				
				if (coordinateSum == null) {
					if (coordinates.getCoordinates() != null) {
						coordinateSum = coordinates.getCoordinates();
					}
					else {
						coordinateSum = new double[thisCoords.length];
					}
				}

				addToCoordVector(coordinateSum, thisCoords);
				count += thisCount;
			}
			
			coordinates.setCoordinates(coordinateSum);
			this.count.setValue(count);
			next.setField(1, coordinates);
			next.setField(2, this.count);
			
			// emit partial sum and partial count for average computation
			out.collect(next);
		}

		/**
		 * Adds two coordinate vectors by summing up each of their coordinates.
		 * 
		 * @param cvToAddTo
		 *        The coordinate vector to which the other vector is added.
		 *        This vector is returned.
		 * @param cvToBeAdded
		 *        The coordinate vector which is added to the other vector.
		 *        This vector is not modified.
		 */
		private void addToCoordVector(double[] cvToAddTo, double[] cvToBeAdded) {

			// check if both vectors have same length
			if (cvToAddTo.length != cvToBeAdded.length) {
				throw new IllegalArgumentException("The given coordinate vectors are not of equal length.");
			}

			// sum coordinate vectors coordinate-wise
			for (int i = 0; i < cvToAddTo.length; i++) {
				cvToAddTo[i] += cvToBeAdded[i];
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args)
	{
		// parse job parameters
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataPointInput = (args.length > 1 ? args[1] : "");
		String clusterInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for data point input
		FileDataSource dataPoints = new FileDataSource(PointInFormat.class, dataPointInput, "Data Points");
		dataPoints.setParameter(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		//dataPoints.addOutputContract(OutputContract.Unique.class);

		// create DataSourceContract for cluster center input
		FileDataSource clusterPoints = new FileDataSource(PointInFormat.class, clusterInput, "Centers");
		clusterPoints.setParameter(DelimitedInputFormat.RECORD_DELIMITER, "\n");
		clusterPoints.setDegreeOfParallelism(1);
		//clusterPoints.addOutputContract(OutputContract.Unique.class);

		// create CrossContract for distance computation
		CrossContract computeDistance = new CrossContract(ComputeDistance.class, dataPoints, clusterPoints, "Compute Distances");
		computeDistance.getCompilerHints().setAvgBytesPerRecord(48);

		// create ReduceContract for finding the nearest cluster centers
		ReduceContract findNearestClusterCenters = new ReduceContract(FindNearestCenter.class, PactInteger.class, 0, computeDistance, "Find Nearest Centers");
		findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(48);

		// create ReduceContract for computing new cluster positions
		ReduceContract recomputeClusterCenter = new ReduceContract(RecomputeClusterCenter.class, PactInteger.class, 0, findNearestClusterCenters, "Recompute Center Positions");
		recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink newClusterPoints = new FileDataSink(PointOutFormat.class, output, recomputeClusterCenter, "New Center Positions");

		// return the PACT plan
		Plan plan = new Plan(newClusterPoints, "KMeans Iteration");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [dataPoints] [clusterCenters] [output]";
	}
}
