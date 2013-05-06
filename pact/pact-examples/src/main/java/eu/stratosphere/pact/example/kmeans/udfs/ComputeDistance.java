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

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Cross PACT computes the distance of all data points to all cluster
 * centers.
 */
@ConstantFieldsFirst({0,1})
public class ComputeDistance extends	CrossStub {
	
	private final PactDouble distance = new PactDouble();
	
	/**
	 * Computes the distance of one data point to one cluster center.
	 * 
	 * Output Format:
	 * 0: pointID
	 * 1: pointVector
	 * 2: clusterID
	 * 3: distance
	 */
	@Override
	public void cross(PactRecord dataPointRecord, PactRecord clusterCenterRecord, Collector<PactRecord> out) {
		
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