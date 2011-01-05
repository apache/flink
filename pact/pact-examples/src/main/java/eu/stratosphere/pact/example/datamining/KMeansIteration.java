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
import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.OutputContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.OutputContract.UniqueKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactPair;

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
public class KMeansIteration implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Implements a coordinate vector of multi-dimensional point. Coordinates
	 * are stored as double values.
	 * 
	 * @author Fabian Hueske
	 */
	public static class CoordVector implements Key, Value {

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
		 * @param coordinates
		 *            The coordinate vector of a multi-dimensional point.
		 */
		public CoordVector(Double[] coordinates) {
			this.coordinates = new double[coordinates.length];
			for (int i = 0; i < coordinates.length; i++) {
				this.coordinates[i] = coordinates[i];
			}
		}

		/**
		 * Initializes a coordinate vector.
		 * 
		 * @param coordinates
		 *            The coordinate vector of a multi-dimensional point.
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
		 * Computes the Euclidian distance between this coordinate vector and a
		 * second coordinate vector.
		 * 
		 * @param cv
		 *            The coordinate vector to which the distance is computed.
		 * @return The Euclidian distance to coordinate vector cv. If cv has a
		 *         different length than this coordinate vector, -1 is returned.
		 */
		public double computeEuclidianDistance(CoordVector cv) {

			// check coordinate vector lengths
			if (cv.coordinates.length != this.coordinates.length) {
				return -1.0;
			}

			double quadSum = 0.0;
			for (int i = 0; i < this.coordinates.length; i++) {
				quadSum += Math.pow((this.coordinates[i] - cv.coordinates[i]), 2);
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
		public int compareTo(Key o) {

			// check if other key is also of type CoordVector
			if (!(o instanceof CoordVector)) {
				return -1;
			}
			// cast to CoordVector
			CoordVector oP = (CoordVector) o;

			// check if both coordinate vectors have identical lengths
			if (oP.coordinates.length > this.coordinates.length) {
				return -1;
			} else if (oP.coordinates.length < this.coordinates.length) {
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
	 * This data type holds the distance from a data point to a cluster center.
	 * Hence, three pieces of information are stored: the coordinate vector of
	 * the data point, the id of the cluster center, and the (Euclidian)
	 * distance between both. The coordinate vector of the data point is later
	 * required to compute the new position of the cluster center.
	 * 
	 * @author Fabian Hueske
	 */
	public static class Distance implements Value {

		CoordVector dataPoint;

		PactInteger clusterId;

		PactDouble distance;

		/**
		 * Initializes a blank Distance object. Required for deserialization.
		 */
		public Distance() {
		}

		/**
		 * Initialized the distance from a data point to a cluster center.
		 * 
		 * @param dataPoint
		 *            The coordinate vector of the data point.
		 * @param clusterId
		 *            The id of the cluster center.
		 * @param distance
		 *            The distance from the data point to the cluster center.
		 */
		public Distance(CoordVector dataPoint, PactInteger clusterId, PactDouble distance) {
			this.dataPoint = dataPoint;
			this.clusterId = clusterId;
			this.distance = distance;
		}

		/**
		 * Returns the coordinate vector of the data point.
		 * 
		 * @return The coordinate vector of the data point.
		 */
		public CoordVector getDataPoint() {
			return dataPoint;
		}

		/**
		 * Returns the id of the cluster center.
		 * 
		 * @return The id of the cluster center.
		 */
		public PactInteger getClusterId() {
			return clusterId;
		}

		/**
		 * Returns the distance from the data point to the cluster center.
		 * 
		 * @return The distance from the data point to the cluster center.
		 */
		public PactDouble getDistance() {
			return distance;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void read(DataInput in) throws IOException {
			dataPoint = new CoordVector();
			dataPoint.read(in);
			clusterId = new PactInteger();
			clusterId.read(in);
			distance = new PactDouble();
			distance.read(in);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			dataPoint.write(out);
			clusterId.write(out);
			distance.write(out);
		}

	}

	/**
	 * Holds a count and a coordinate vector. This type is required for the
	 * Combiner of the second Reduce PACT and hold pre-aggregated average value
	 * of the new coordinate vector (position) of a cluster center. This class
	 * extends N_Pair.
	 * 
	 * @author Fabian Hueske
	 */
	public static class CoordVectorCountSum extends PactPair<PactInteger, CoordVector> {

		/**
		 * Initializes a blank CoordVectorCountSum. Required for
		 * deserialization.
		 */
		public CoordVectorCountSum() {
			super();
		}

		/**
		 * Initializes a CoordVectorCountSum.
		 * 
		 * @param count
		 *            The count value of the pre-aggregated average value.
		 * @param pointSum
		 *            The sum value of the pre-aggregated average value.
		 */
		public CoordVectorCountSum(PactInteger count, CoordVector pointSum) {
			super(count, pointSum);
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
	public static class PointInFormat extends TextInputFormat<PactInteger, CoordVector> {

		// TODO: Decide whether to use performance reader or easy-to-understand
		// reader
		@Override
		public boolean readLine(KeyValuePair<PactInteger, CoordVector> pair, byte[] line) {

			int id = -1;
			int value = 0;
			int fractionValue = 0;
			int fractionChars = 0;

			ArrayList<Double> coordinates = new ArrayList<Double>();

			for (int pos = 0; pos < line.length; pos++) {
				if (line[pos] == '|') {
					// check if id was already set
					if (id == -1) {
						id = value;
					} else {
						coordinates.add(value + ((double) fractionValue) * Math.pow(10, (-1 * (fractionChars - 1))));
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

			pair.setKey(new PactInteger(id));
			Double[] dA = new Double[coordinates.size()];
			pair.setValue(new CoordVector(coordinates.toArray(dA)));
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
	public static class PointOutFormat extends TextOutputFormat<PactInteger, CoordVector> {
		@Override
		public byte[] writeLine(KeyValuePair<PactInteger, CoordVector> pair) {
			StringBuilder line = new StringBuilder();
			DecimalFormat df = new DecimalFormat("####0.00");

			line.append(pair.getKey().getValue());

			for (double coord : pair.getValue().getCoordinates()) {
				line.append('|');
				line.append(df.format(coord));
			}
			line.append('|');
			line.append('\n');

			return line.toString().getBytes();
		}

	}

	/**
	 * Cross PACT computes the distance of all data points to all cluster
	 * centers. The SameKeyFirst OutputContract is annotated because PACTs
	 * output key is the key of the first input (data points).
	 * 
	 * @author Fabian Hueske
	 */
	@OutputContract.SameKeyFirst
	public static class ComputeDistance extends
			CrossStub<PactInteger, CoordVector, PactInteger, CoordVector, PactInteger, Distance> {

		/**
		 * Computes the distance of one data point to one cluster center and
		 * emits a key-value-pair where the id of the data point is the key and
		 * a Distance object is the value.
		 */
		@Override
		public void cross(PactInteger pid, CoordVector dataPoint, PactInteger cid, CoordVector clusterPoint,
				Collector<PactInteger, Distance> out) {

			// compute Euclidian distance and create Distance object
			Distance distance = new Distance(dataPoint, cid, new PactDouble(dataPoint
					.computeEuclidianDistance(clusterPoint)));

			// emit key-value-pair with distance information
			out.collect(pid, distance);
		}
	}

	/**
	 * Reduce PACT determines the closes cluster center for a data point. This
	 * is a minimum aggregation. Hence, a Combiner can be easily implemented.
	 * 
	 * @author Fabian Hueske
	 */
	@Combinable
	public static class FindNearestCenter extends ReduceStub<PactInteger, Distance, PactInteger, CoordVectorCountSum> {

		/**
		 * Computes a minimum aggregation on the distance of a data point to
		 * cluster centers. Emits a key-value-pair where the key is the id of
		 * the closest cluster center and the value is the coordinate vector of
		 * the data point. The CoordVectorCountSum data type is used to enable
		 * the use of a Combiner for the second Reduce PACT.
		 */
		@Override
		public void reduce(PactInteger pid, Iterator<Distance> distancesList,
				Collector<PactInteger, CoordVectorCountSum> out) {

			// initialize nearest cluster with the first distance
			Distance nearestCluster = null;
			if (distancesList.hasNext()) {
				nearestCluster = distancesList.next();
			} else {
				return;
			}

			// check all cluster centers
			while (distancesList.hasNext()) {
				Distance distance = distancesList.next();

				// compare distances
				if (distance.getDistance().getValue() < nearestCluster.getDistance().getValue()) {
					// if distance is smaller than smallest till now, update
					// nearest cluster
					nearestCluster = distance;
				}
			}

			// emit a key-value-pair where the cluster center id is the key and
			// the coordinate vector of the data point is the value. The
			// CoordVectorCountSum data type is used to enable the use of a
			// Combiner for the second Reduce PACT.
			out.collect(nearestCluster.getClusterId(), new CoordVectorCountSum(new PactInteger(1), nearestCluster
					.getDataPoint()));
		}

		/**
		 * Computes a minimum aggregation on the distance of a data point to
		 * cluster centers.
		 */
		@Override
		public void combine(PactInteger pid, Iterator<Distance> distancesList, Collector<PactInteger, Distance> out) {

			// initialize nearest cluster with the first distance
			Distance nearestCluster = null;
			if (distancesList.hasNext()) {
				nearestCluster = distancesList.next();
			} else {
				return;
			}

			// check all cluster centers
			while (distancesList.hasNext()) {
				Distance distance = distancesList.next();

				// compare distance
				if (distance.getDistance().getValue() < nearestCluster.getDistance().getValue()) {
					// if distance is smaller than smallest till now, update
					// nearest cluster
					nearestCluster = distance;
				}
			}

			// emit nearest cluster
			out.collect(pid, nearestCluster);
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
	@SameKey
	@Combinable
	public static class RecomputeClusterCenter extends
			ReduceStub<PactInteger, CoordVectorCountSum, PactInteger, CoordVector> {

		/**
		 * Compute the new position (coordinate vector) of a cluster center.
		 */
		@Override
		public void reduce(PactInteger cid, Iterator<CoordVectorCountSum> dataPoints,
				Collector<PactInteger, CoordVector> out) {

			CoordVectorCountSum pcs;

			// initialize coordinate vector sum and count
			double[] coordinateSum = null;
			long count = 0;
			if (dataPoints.hasNext()) {
				pcs = dataPoints.next();
				coordinateSum = pcs.getSecond().getCoordinates();
				count = pcs.getFirst().getValue();
			}

			// compute coordiante vector sum and count
			while (dataPoints.hasNext()) {

				// get next data point and count
				pcs = dataPoints.next();

				coordinateSum = addToCoordVector(coordinateSum, pcs.getSecond().getCoordinates());
				count += pcs.getFirst().getValue();
			}

			// compute new coordinate vector (position) of cluster center
			for (int i = 0; i < coordinateSum.length; i++) {
				coordinateSum[i] /= count;
			}
			CoordVector newClusterPoint = new CoordVector(coordinateSum);

			// emit new position of cluster center
			out.collect(cid, newClusterPoint);

		}

		/**
		 * Computes a pre-aggregated average value of a coordinate vector.
		 */
		@Override
		public void combine(PactInteger cid, Iterator<CoordVectorCountSum> dataPoints,
				Collector<PactInteger, CoordVectorCountSum> out) {

			CoordVectorCountSum pcs;

			// initialize coordinate vector sum and count
			double[] coordinateSum = null;
			int count = 0;
			if (dataPoints.hasNext()) {
				pcs = dataPoints.next();
				coordinateSum = pcs.getSecond().getCoordinates();
				count = pcs.getFirst().getValue();
			}

			// compute coordiante vector sum and count
			while (dataPoints.hasNext()) {

				// get next data point and count
				pcs = dataPoints.next();

				coordinateSum = addToCoordVector(coordinateSum, pcs.getSecond().getCoordinates());
				count += pcs.getFirst().getValue();
			}

			// emit partial sum and partial count for average computation
			out.collect(cid, new CoordVectorCountSum(new PactInteger(count), new CoordVector(coordinateSum)));

		}

		/**
		 * Sums two coordiante vectors by summing up each of their coordinates.
		 * 
		 * @param cvToAddTo
		 *            The coordinate vector to which the other vector is added.
		 *            This vector is returned.
		 * @param cvToBeAdded
		 *            The coordinate vector which is added to the other vector.
		 *            This vector is not modified.
		 * @return Null if the coordinate vectors differ in their lengths.
		 *         Otherwise, the coordinate vector to which the other vector
		 *         was added.
		 */
		private double[] addToCoordVector(double[] cvToAddTo, double[] cvToBeAdded) {

			// check if both vectors have same length
			if (cvToAddTo.length != cvToBeAdded.length) {
				return null;
			}

			// sum coordinate vectors coordinate-wise
			for (int i = 0; i < cvToAddTo.length; i++) {
				cvToAddTo[i] += cvToBeAdded[i];
			}

			// return the coordiante vector to which was added.
			return cvToAddTo;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		int noSubTasks = 1;
		String dataPointInput = "";
		String clusterInput = "";
		String output = "";

		// check for the correct number of job parameters
		if (args.length == 4) {
			// parse job parameters
			noSubTasks = Integer.parseInt(args[0]);
			dataPointInput = args[1];
			clusterInput = args[2];
			output = args[3];
		}

		// create DataSourceContract for data point input
		DataSourceContract<PactInteger, CoordVector> dataPoints = new DataSourceContract<PactInteger, CoordVector>(
				PointInFormat.class, dataPointInput, "Read Data Points");
		dataPoints.setFormatParameter("delimiter", "\n");
		dataPoints.setDegreeOfParallelism(noSubTasks);
		dataPoints.setOutputContract(UniqueKey.class);

		// create DataSourceContract for cluster center input
		DataSourceContract<PactInteger, CoordVector> clusterPoints = new DataSourceContract<PactInteger, CoordVector>(
				PointInFormat.class, clusterInput, "Read Centers");
		clusterPoints.setFormatParameter("delimiter", "\n");
		clusterPoints.setDegreeOfParallelism(1);
		clusterPoints.setOutputContract(UniqueKey.class);

		// create CrossContract for distance computation
		CrossContract<PactInteger, CoordVector, PactInteger, CoordVector, PactInteger, Distance> computeDistance = new CrossContract<PactInteger, CoordVector, PactInteger, CoordVector, PactInteger, Distance>(
				ComputeDistance.class, "Compute Distances");
		computeDistance.setDegreeOfParallelism(noSubTasks);
		computeDistance.getCompilerHints().setAvgBytesPerRecord(48);

		// create ReduceContract for finding the nearest cluster centers
		ReduceContract<PactInteger, Distance, PactInteger, CoordVectorCountSum> findNearestClusterCenters = new ReduceContract<PactInteger, Distance, PactInteger, CoordVectorCountSum>(
				FindNearestCenter.class, "Find Nearest Centers");
		findNearestClusterCenters.setDegreeOfParallelism(noSubTasks);
		findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(48);

		// create ReduceContract for computing new cluster positions
		ReduceContract<PactInteger, CoordVectorCountSum, PactInteger, CoordVector> recomputeClusterCenter = new ReduceContract<PactInteger, CoordVectorCountSum, PactInteger, CoordVector>(
				RecomputeClusterCenter.class, "Recompute Center Positions");
		recomputeClusterCenter.setDegreeOfParallelism(noSubTasks);
		recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36);

		// create DataSinkContract for writing the new cluster positions
		DataSinkContract<PactInteger, CoordVector> newClusterPoints = new DataSinkContract<PactInteger, CoordVector>(
				PointOutFormat.class, output, "Write new Center Positions");
		newClusterPoints.setDegreeOfParallelism(noSubTasks);

		// assemble the PACT plan
		newClusterPoints.setInput(recomputeClusterCenter);
		recomputeClusterCenter.setInput(findNearestClusterCenters);
		findNearestClusterCenters.setInput(computeDistance);
		computeDistance.setFirstInput(dataPoints);
		computeDistance.setSecondInput(clusterPoints);

		// return the PACT plan
		return new Plan(newClusterPoints, "KMeans Iteration");

	}

	@Override
	public String getDescription() {
		return "Parameters: [dop] [data-points] [cluster-centers] [output]";
	}

}
