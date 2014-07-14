/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.faulttolerance;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class ExactlyOnceFaultToleranceBuffer extends FaultToleranceBuffer {

	private Map<String, int[]> ackCounter;
	int[] initialAckCounts;

	public ExactlyOnceFaultToleranceBuffer(int[] numberOfChannels, String componentInstanceID) {
		super(numberOfChannels, componentInstanceID);
		this.ackCounter = new HashMap<String, int[]>();
		this.initialAckCounts = new int[numberOfEffectiveChannels.length + 1];
		for (int i = 0; i < numberOfEffectiveChannels.length; i++) {
			this.initialAckCounts[i + 1] = numberOfEffectiveChannels[i];
		}
	}

	@Override
	protected void addToAckCounter(String id) {
		ackCounter.put(id, Arrays.copyOf(initialAckCounts, numberOfEffectiveChannels.length + 1));
	}
	
	
	private void addToAckCounter(String id, int channel) {
		int[] acks=new int[numberOfEffectiveChannels.length + 1];
		acks[0]=numberOfEffectiveChannels.length-1;
		acks[channel+1]=numberOfEffectiveChannels[channel];
		ackCounter.put(id, acks);
	}

	@Override
	protected boolean removeFromAckCounter(String id) {
		return (ackCounter.remove(id) != null);
	}

	@Override
	protected void ack(String id, int channel) {

		int[] acks = ackCounter.get(id);

		if (acks != null) {
			if (decreaseAckCounter(acks, channel)) {
				remove(id);
			}
		}
	}

	private boolean decreaseAckCounter(int[] acks, int channel) {

		acks[channel + 1]--;
		if (acks[channel + 1] == 0) {
			acks[0]++;
		}

		return (acks[0] == numberOfEffectiveChannels.length);
	}

	@Override
	protected StreamRecord failChannel(String id, int channel) {
		
		if(notAcked(id, channel)){
			
			int[] acks = ackCounter.get(id);
			acks[channel + 1] = 0;
			acks[0]++;
			
			if(acks[0]==numberOfEffectiveChannels.length){
				remove(id);
			}
			
			return addToChannel(id, channel);
		} else{
			
			return null;
		}
		
	}
	
	private StreamRecord addToChannel(String id, int channel) {

		StreamRecord record = recordBuffer.get(id).copy().setId(componentInstanceID);

		String new_id = record.getId();
		recordBuffer.put(new_id, record);
		addTimestamp(new_id);

		addToAckCounter(new_id,channel);
		return record;
	}

	private boolean notAcked(String id, int channel) {
		int[] acks = ackCounter.get(id);
		if (acks != null) {
			if(acks[channel+1]>0){
				return true;
			}	
		} 
		
		return false;
	
	}

}
