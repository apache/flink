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
package eu.stratosphere.pact.example.kmeans.udfs;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Reduce PACT determines the closes cluster center for a data point. This
 * is a minimum aggregation. Hence, a Combiner can be easily implemented.
 */
@Combinable
@ConstantFields(1)
public class FindNearestCenter extends ReduceStub implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final PactInteger centerId = new PactInteger();
	private final CoordVector position = new CoordVector();
	private final PactInteger one = new PactInteger(1);
	
	private final PactRecord result = new PactRecord(3);
	
	/**
	 * Computes a minimum aggregation on the distance of a data point to
	 * cluster centers. 
	 * 
	 * Output Format:
	 * 0: centerID
	 * 1: pointVector
	 * 2: constant(1) (to enable combinable average computation in the following reducer)
	 */
	@Override
	public void reduce(Iterator<PactRecord> pointsWithDistance, Collector<PactRecord> out) {
		double nearestDistance = Double.MAX_VALUE;
		int nearestClusterId = 0;

		// check all cluster centers
		while (pointsWithDistance.hasNext()) {
			PactRecord res = pointsWithDistance.next();
			
			double distance = res.getField(3, PactDouble.class).getValue();

			// compare distances
			if (distance < nearestDistance) {
				// if distance is smaller than smallest till now, update nearest cluster
				nearestDistance = distance;
				nearestClusterId = res.getField(2, PactInteger.class).getValue();
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
	public void combine(Iterator<PactRecord> pointsWithDistance, Collector<PactRecord> out) {
		double nearestDistance = Double.MAX_VALUE;

		// check all cluster centers
		while (pointsWithDistance.hasNext()) {
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