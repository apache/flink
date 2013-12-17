/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.example.record.kmeans.udfs;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.api.record.functions.ReduceFunction;
import eu.stratosphere.api.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

/**
 * Reduce PACT computes the new position (coordinate vector) of a cluster
 * center. This is an average computation. Hence, Combinable is annotated
 * and the combine method implemented. 
 * 
 * Output Format:
 * 0: clusterID
 * 1: clusterVector
 */
@Combinable
@ConstantFields(0)
public class RecomputeClusterCenter extends ReduceFunction implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final PactInteger count = new PactInteger();
	
	/**
	 * Compute the new position (coordinate vector) of a cluster center.
	 */
	@Override
	public void reduce(Iterator<PactRecord> dataPoints, Collector<PactRecord> out) {
		PactRecord next = null;
			
		// initialize coordinate vector sum and count
		CoordVector coordinates = new CoordVector();
		double[] coordinateSum = null;
		int count = 0;	

		// compute coordinate vector sum and count
		while (dataPoints.hasNext()) {
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
		next.setNull(2);

		// emit new position of cluster center
		out.collect(next);
	}

	/**
	 * Computes a pre-aggregated average value of a coordinate vector.
	 */
	@Override
	public void combine(Iterator<PactRecord> dataPoints, Collector<PactRecord> out) {
		
		PactRecord next = null;
		
		// initialize coordinate vector sum and count
		CoordVector coordinates = new CoordVector();
		double[] coordinateSum = null;
		int count = 0;	

		// compute coordinate vector sum and count
		while (dataPoints.hasNext()) {
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