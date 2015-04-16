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

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.io.Serializable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/*
 * This class tracks heavy hitters using the {@link com.clearspring.analytics.stream.frequency.CountMinSketch} structure
 * to estimate frequencies.
 */
public class CountMinHeavyHitter implements HeavyHitter, Serializable {

	private transient CountMinSketch countMinSketch;
	private HashMap<Object,Long> heavyHitters;
	private double fraction;
	private double error;
	private long cardinality;

	public CountMinHeavyHitter(double fraction, double error, double confidence, int seed){
		this.countMinSketch = new CountMinSketch(error,confidence,seed);
		this.error = error;
		this.cardinality = 0;
		this.fraction = fraction;
		this.heavyHitters = new HashMap<Object,Long>();
	}

	public CountMinHeavyHitter(CountMinSketch countMinSketch, double fraction){
		this.countMinSketch = countMinSketch;
		this.error = countMinSketch.getRelativeError();
		this.cardinality = 0;
		this.fraction = fraction;
		this.heavyHitters = new HashMap<Object,Long>();
	}

	@Override
	public void addObject(Object o) {
		cardinality +=1;
		if (o instanceof Long){
			countMinSketch.add((Long)o, 1);
		}else{
			countMinSketch.add(MurmurHash.hash(o), 1);
		}
		updateHeavyHitters(o);
	}

	private void updateHeavyHitters(Object item){
		long minFrequency = (long)Math.ceil(cardinality * fraction);
		long estimateCount = estimateCount(item);

		if (estimateCount >= minFrequency){
			heavyHitters.put(item, estimateCount);
		}

		if (cardinality%(long)Math.ceil(1/error)==0) {
			removeNonFrequent(minFrequency);
		}
	}

	private void removeNonFrequent(long minFrequency){
		Iterator it = heavyHitters.entrySet().iterator();
		while (it.hasNext()) {
			if (((Map.Entry<Object,Long>)it.next()).getValue() < minFrequency) {
				it.remove();
			}
		}
	}

	public long estimateCount(Object item){
		if (item instanceof Long){
			return countMinSketch.estimateCount((Long)item);
		}else{
			return countMinSketch.estimateCount(MurmurHash.hash(item));
		}
	}

	public void merge(HeavyHitter toMerge) throws CMHeavyHitterMergeException {

		try {
			CountMinHeavyHitter cmToMerge = (CountMinHeavyHitter)toMerge;
			if (this.fraction != cmToMerge.fraction) {
				throw new CMHeavyHitterMergeException("The fraction for both heavy hitters must be the same");
			}

			this.countMinSketch = CountMinSketch.merge(this.countMinSketch, cmToMerge.countMinSketch);

			HashMap<Object,Long> mergedHeavyHitters = new HashMap<Object, Long>();

			for (Map.Entry<Object, Long> entry : this.heavyHitters.entrySet()) {
				mergedHeavyHitters.put(entry.getKey(), estimateCount(entry.getKey()));
			}

			for (Map.Entry<Object, Long> entry : cmToMerge.heavyHitters.entrySet()) {
				if (!mergedHeavyHitters.containsKey(entry.getKey())) {
					mergedHeavyHitters.put(entry.getKey(), estimateCount(entry.getKey()));
				}
			}
			this.heavyHitters = mergedHeavyHitters;
			cardinality+=cmToMerge.cardinality;

		}catch (ClassCastException ex){
			throw new CMHeavyHitterMergeException("Both heavy hitter objects must belong to the same class");
		}catch (Exception ex){
			throw new CMHeavyHitterMergeException("Cannot merge count min sketches: "+ex.getMessage());
		}
	}


	@Override
	public HashMap<Object,Long> getHeavyHitters() {
		long minFrequency = (long)Math.ceil(cardinality * fraction);
		removeNonFrequent(minFrequency);
		return heavyHitters;
	}

	protected static class CMHeavyHitterMergeException extends HeavyHitterMergeException {
		public CMHeavyHitterMergeException(String message) {
			super(message);
		}
	}

	@Override
	public String toString(){
		String out = "";
		Map<Object, Long> heavyHitters = getHeavyHitters();
		for (Map.Entry<Object, Long> entry : heavyHitters.entrySet()){
			out += entry.getKey().toString() + " -> estimated freq. " + entry.getValue() + "\n";
		}
		return out;
	}

	private void writeObject(ObjectOutputStream oos) throws IOException{
		oos.defaultWriteObject();
		oos.writeObject(CountMinSketch.serialize(countMinSketch));
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		byte[] countMinBytes = (byte[]) in.readObject();
		countMinSketch = CountMinSketch.deserialize(countMinBytes);
	}

}
