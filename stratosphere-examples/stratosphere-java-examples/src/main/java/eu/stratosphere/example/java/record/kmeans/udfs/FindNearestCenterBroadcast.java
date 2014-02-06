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
import java.util.Collection;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * Determines the closest cluster center for a data point.
 */
@ConstantFieldsFirst({0,1})
public class FindNearestCenterBroadcast extends MapFunction implements Serializable {
	private static final long serialVersionUID = 1L;

	private final IntValue centerId = new IntValue();
	private final CoordVector dataPoint = new CoordVector();
	private final CoordVector centerPoint = new CoordVector();
	private final IntValue one = new IntValue(1);

	private final Record result = new Record(3);

	private Collection<Record> clusterCenters;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.clusterCenters = this.getRuntimeContext().getBroadcastVariable("centers");
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
		dataPointRecord.getFieldInto(1, this.dataPoint);
		
		double nearestDistance = Double.MAX_VALUE;

		// check all cluster centers
		for (Record clusterCenterRecord : this.clusterCenters) {
			clusterCenterRecord.getFieldInto(1, this.centerPoint);

			// compute distance
			double distance = this.dataPoint.computeEuclidianDistance(this.centerPoint);
			// update nearest cluster if necessary 
			if (distance < nearestDistance) {
				nearestDistance = distance;
				clusterCenterRecord.getFieldInto(0, this.centerId);
			}
		}

		// emit a new record with the center id and the data point. add a one to ease the
		// implementation of the average function with a combiner
		this.result.setField(0, this.centerId);
		this.result.setField(1, this.dataPoint);
		this.result.setField(2, this.one);

		out.collect(this.result);
	}
}