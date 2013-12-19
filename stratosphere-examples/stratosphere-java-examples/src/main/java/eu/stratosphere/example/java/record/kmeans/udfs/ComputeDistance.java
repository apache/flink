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
package eu.stratosphere.example.java.record.kmeans.udfs;

import java.io.Serializable;

import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * Cross PACT computes the distance of all data points to all cluster
 * centers.
 */
@ConstantFieldsFirst({0,1})
public class ComputeDistance extends CrossFunction implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final DoubleValue distance = new DoubleValue();
	
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
	public void cross(Record dataPointRecord, Record clusterCenterRecord, Collector<Record> out) {
		
		CoordVector dataPoint = dataPointRecord.getField(1, CoordVector.class);
		
		IntValue clusterCenterId = clusterCenterRecord.getField(0, IntValue.class);
		CoordVector clusterPoint = clusterCenterRecord.getField(1, CoordVector.class);
	
		this.distance.setValue(dataPoint.computeEuclidianDistance(clusterPoint));
		
		// add cluster center id and distance to the data point record 
		dataPointRecord.setField(2, clusterCenterId);
		dataPointRecord.setField(3, this.distance);
		
		out.collect(dataPointRecord);
	}
}