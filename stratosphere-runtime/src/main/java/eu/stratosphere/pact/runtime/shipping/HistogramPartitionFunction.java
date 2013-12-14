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

package eu.stratosphere.pact.runtime.shipping;

import java.util.Arrays;

import eu.stratosphere.api.operators.Order;
import eu.stratosphere.types.PactRecord;

public class HistogramPartitionFunction implements PartitionFunction {
	private final PactRecord[] splitBorders;
	private final Order partitionOrder;
	
	public HistogramPartitionFunction(PactRecord[] splitBorders, Order partitionOrder) {
		this.splitBorders = splitBorders;
		this.partitionOrder = partitionOrder;
	}

	@Override
	public void selectChannels(PactRecord data, int numChannels, int[] channels) {
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
