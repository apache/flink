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

package org.apache.flink.test.recordJobs.kmeans.udfs;

import java.io.Serializable;

import org.apache.flink.api.java.record.functions.CrossFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;

/**
 * Cross PACT computes the distance of all data points to all cluster
 * centers.
 */
@SuppressWarnings("deprecation")
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
	public Record cross(Record dataPointRecord, Record clusterCenterRecord) throws Exception {

		CoordVector dataPoint = dataPointRecord.getField(1, CoordVector.class);

		IntValue clusterCenterId = clusterCenterRecord.getField(0, IntValue.class);
		CoordVector clusterPoint = clusterCenterRecord.getField(1, CoordVector.class);

		this.distance.setValue(dataPoint.computeEuclidianDistance(clusterPoint));

		// add cluster center id and distance to the data point record
		dataPointRecord.setField(2, clusterCenterId);
		dataPointRecord.setField(3, this.distance);

		return dataPointRecord;
	}
}
