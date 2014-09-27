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


package org.apache.flink.runtime.operators.shipping;

import java.util.Arrays;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.types.Record;

public class HistogramPartitionFunction implements PartitionFunction {
	private final Record[] splitBorders;
	private final Order partitionOrder;
	
	public HistogramPartitionFunction(Record[] splitBorders, Order partitionOrder) {
		this.splitBorders = splitBorders;
		this.partitionOrder = partitionOrder;
	}

	@Override
	public void selectChannels(Record data, int numChannels, int[] channels) {
		//TODO: Check partition borders match number of channels
		int pos = Arrays.binarySearch(splitBorders, data);

		/*
		 * 
		 * TODO CHECK ONLY FOR KEYS NOT FOR WHOLE RECORD
		 * 
		 */
		
		if(pos < 0) {
			pos++;
			pos = -pos;
		}
		
		if(partitionOrder == Order.ASCENDING || partitionOrder == Order.ANY) {
			channels[0] = pos;
		} else {
			channels[0] = splitBorders.length  - pos;
		}
	}
}
