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

package eu.stratosphere.pact.runtime.task.util;

import java.util.Arrays;

import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.type.Key;

public class HistogramPartitionFunction implements PartitionFunction {
	private final Key[] splitBorders;
	private final Order partitionOrder;
	private final int[] channels = new int[1];
	
	public HistogramPartitionFunction(Key[] splitBorders, Order partitionOrder) {
		this.splitBorders = splitBorders;
		this.partitionOrder = partitionOrder;
	}

	@Override
	public int[] selectChannels(Key data, int numChannels) {
		//TODO: Check partition borders match number of channels
		int pos = Arrays.binarySearch(splitBorders, data);
		if(pos < 0) {
			pos++;
			pos = -pos;
		}
		
		if(partitionOrder == Order.ASCENDING || partitionOrder == Order.ANY) {
			channels[0] = pos;
		} else {
			channels[0] = splitBorders.length  - pos;
		}
		
		return channels;
	}
}
