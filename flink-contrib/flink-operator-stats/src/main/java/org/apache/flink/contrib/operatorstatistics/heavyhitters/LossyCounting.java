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

package org.apache.flink.contrib.operatorstatistics.heavyhitters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implements Lossy Counting algorithm (Manku, G.S., Motwani, R.: Approximate frequency counts over data streams, 2002)
 * The algorithm tracks heavy hitters in a count based fashion. It stores heavy hitters, along with a lower bound for
 * their frequency and an error, which determines the upperbound for the frequency. It is guaranteed to output all
 * elements with freq. higher than a given threshold and to not output any element with a frequency under a given
 * error threshold.
 */
public class LossyCounting implements HeavyHitter, Serializable{

	private double fraction;
	private double error;
	private long cardinality;
	private Map<Object,Counter> heavyHitters;
	private long bucket;

	private class Counter implements Serializable {
		long lowerBound;
		long frequencyError;

		private Counter(long lowerBound, long frequencyError){
			this.lowerBound = lowerBound;
			this.frequencyError = frequencyError;
		}

		private void updateLowerBound(long count){
			lowerBound+=count;
		}

		private long getUpperBound(){
			return lowerBound + frequencyError;
		}

	}

	public LossyCounting(double fraction, double error){

		this.fraction = fraction;
		this.error = error;
		this.cardinality = 0;
		this.heavyHitters = new HashMap<Object, Counter>();
		this.bucket = 0;
	}

	@Override
	public void addObject(Object o) {
		cardinality+=1;
		if (heavyHitters.containsKey(o)){
			heavyHitters.get(o).updateLowerBound(1);
		}else{
			heavyHitters.put(o,new Counter(1, bucket));
		}
		if (cardinality%(long)Math.ceil(1/error)==0) {
			bucket += 1;
			updateHeavyHitters();
		}
	}

	public void updateHeavyHitters(){
		Iterator it = heavyHitters.entrySet().iterator();
		while (it.hasNext()) {
			if (((Map.Entry<Object,Counter>)it.next()).getValue().getUpperBound()< bucket) {
				it.remove();
			}
		}
	}

	public void merge(HeavyHitter toMerge) throws HeavyHitterMergeException {
		try{
			LossyCounting lsToMerge = (LossyCounting)toMerge;
			if (this.fraction!=lsToMerge.fraction){
				throw new HeavyHitterMergeException("Both heavy hitter structures must be identical");
			}
			this.cardinality+=lsToMerge.cardinality;
			this.bucket = (long)Math.floor(cardinality*error);
			for (Map.Entry<Object, Counter> entry : lsToMerge.heavyHitters.entrySet()){
				Counter counter = this.heavyHitters.get(entry.getKey());
				if (counter==null){
					this.heavyHitters.put(entry.getKey(),entry.getValue());
				}else{
					Counter mergingCounter = entry.getValue();
					this.heavyHitters.put(entry.getKey(),
							new Counter(mergingCounter.lowerBound+counter.lowerBound, mergingCounter.frequencyError +counter.frequencyError));
				}
			}
			updateHeavyHitters();
		}catch (ClassCastException ex){
			throw new HeavyHitterMergeException("Both heavy hitter structures must be identical");
		}
	}

	@Override
	public HashMap<Object,Long> getHeavyHitters() {
		HashMap<Object,Long> heavyHitterLowerBounds = new HashMap<Object, Long>();
		long minFrequency = (long)Math.ceil(cardinality*(fraction-error));
		for (Map.Entry<Object, Counter> entry : heavyHitters.entrySet()){
			if(entry.getValue().lowerBound>=minFrequency){
				heavyHitterLowerBounds.put(entry.getKey(), entry.getValue().lowerBound);
			}
		}
		return heavyHitterLowerBounds;
	}

	@Override
	public String toString(){
		String out = "";
		long minFrequency = (long)Math.ceil(cardinality*(fraction-error));
		for (Map.Entry<Object, Counter> entry : heavyHitters.entrySet()){
			if(entry.getValue().lowerBound>=minFrequency) {
				out += entry.getKey().toString() + " -> lower bound " + entry.getValue().lowerBound + "\n";
			}
		}
		return out;
	}

}
