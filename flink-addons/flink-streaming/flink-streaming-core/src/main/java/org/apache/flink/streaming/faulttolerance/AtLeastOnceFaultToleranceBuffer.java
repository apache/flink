/***********************************************************************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **********************************************************************************************************************/

package org.apache.flink.streaming.faulttolerance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.UID;

public class AtLeastOnceFaultToleranceBuffer extends FaultToleranceBuffer {
	
	protected Map<UID, Integer> ackCounter;

	public AtLeastOnceFaultToleranceBuffer(int[] numberOfChannels, int componentInstanceID) {
		super(numberOfChannels, componentInstanceID);
		this.ackCounter = new ConcurrentHashMap<UID, Integer>();
	}
	

	@Override
	protected void addToAckCounter(UID id) {
		ackCounter.put(id, totalNumberOfEffectiveChannels);
	}

	@Override
	protected boolean removeFromAckCounter(UID id) {
		return (ackCounter.remove(id) != null);
	}

	@Override
	protected void ack(UID id, int channel) {
		Integer ackCount = ackCounter.get(id);

		if (ackCount != null) {

			ackCount--;

			if (ackCount == 0) {
				remove(id);
			} else {
				ackCounter.put(id, ackCount);
			}
		}
	}

	@Override
	protected StreamRecord failChannel(UID id, int channel) {
		return fail(id);
	}

}
