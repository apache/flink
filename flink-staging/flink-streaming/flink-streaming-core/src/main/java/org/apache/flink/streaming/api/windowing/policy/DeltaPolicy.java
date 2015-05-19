/*
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
 */

package org.apache.flink.streaming.api.windowing.policy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;

/**
 * This policy calculates a delta between the data point which triggered last
 * and the currently arrived data point. It triggers if the delta is higher than
 * a specified threshold.
 * 
 * In case it gets used for eviction, this policy starts from the first element
 * of the buffer and removes all elements from the buffer which have a higher
 * delta then the threshold. As soon as there is an element with a lower delta,
 * the eviction stops.
 * 
 * By default this policy does not react on fake elements. Wrap it in an
 * {@link ActiveEvictionPolicyWrapper} to make it calculate the delta even on
 * fake elements.
 * 
 * @param <DATA>
 *            The type of the data points which are handled by this policy
 */
public class DeltaPolicy<DATA> implements CloneableTriggerPolicy<DATA>,
		CloneableEvictionPolicy<DATA> {

	/**
	 * Auto generated version ID
	 */
	private static final long serialVersionUID = -7797538922123394967L;

	//Used for serializing the threshold
	private final static int INITIAL_SERIALIZER_BYTES = 1024;

	protected DeltaFunction<DATA> deltaFuntion;
	private List<DATA> windowBuffer;
	protected double threshold;
	private TypeSerializer<DATA> typeSerializer;
	protected transient DATA triggerDataPoint;

	/**
	 * Creates a delta policy which calculates a delta between the data point
	 * which triggered last and the currently arrived data point. It triggers if
	 * the delta is higher than a specified threshold. As the data may be sent to
	 * the cluster a {@link TypeSerializer} is needed for the initial value.
	 *
	 * <p>
	 * In case it gets used for eviction, this policy starts from the first
	 * element of the buffer and removes all elements from the buffer which have
	 * a higher delta then the threshold. As soon as there is an element with a
	 * lower delta, the eviction stops.
	 * </p>
	 *
	 * @param deltaFuntion
	 * 				The delta function to be used.
	 * @param init
	 *				The initial to be used for the calculation of a delta before
	 *				the first trigger.
	 * @param threshold
	 * 				The threshold upon which a triggering should happen.
	 * @param typeSerializer
	 * 				TypeSerializer to properly forward the initial value to
	 * 				the cluster
	 */
	public DeltaPolicy(DeltaFunction<DATA> deltaFuntion, DATA init, double threshold, TypeSerializer typeSerializer) {
		this.deltaFuntion = deltaFuntion;
		this.triggerDataPoint = init;
		this.windowBuffer = new LinkedList<DATA>();
		this.threshold = threshold;
		this.typeSerializer = typeSerializer;
	}

	@Override
	public boolean notifyTrigger(DATA datapoint) {
		if (deltaFuntion.getDelta(this.triggerDataPoint, datapoint) > this.threshold) {
			this.triggerDataPoint = datapoint;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize) {
		windowBuffer = windowBuffer.subList(windowBuffer.size() - bufferSize, bufferSize);
		int evictCount = 0;
		for (DATA bufferPoint : windowBuffer) {
			if (deltaFuntion.getDelta(bufferPoint, datapoint) < this.threshold) {
				break;
			}
			evictCount++;
		}

		if (evictCount > 0) {
			windowBuffer = windowBuffer.subList(evictCount, windowBuffer.size());
		}
		windowBuffer.add(datapoint);
		return evictCount;
	}

	@Override
	public DeltaPolicy<DATA> clone() {
		return new DeltaPolicy<DATA>(deltaFuntion, triggerDataPoint, threshold, typeSerializer);
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DeltaPolicy)) {
			return false;
		} else {
			try {
				@SuppressWarnings("unchecked")
				DeltaPolicy<DATA> otherPolicy = (DeltaPolicy<DATA>) other;
				return threshold == otherPolicy.threshold
						&& deltaFuntion.getClass() == otherPolicy.deltaFuntion.getClass()
						&& triggerDataPoint.equals(otherPolicy.triggerDataPoint);
			} catch (ClassCastException e) {
				return false;
			}
		}
	}

	@Override
	public String toString() {
		return "DeltaPolicy(" + threshold + ", " + deltaFuntion.getClass().getSimpleName() + ")";
	}

	private void writeObject(ObjectOutputStream stream) throws IOException{
		stream.defaultWriteObject();
		DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(INITIAL_SERIALIZER_BYTES);
		typeSerializer.serialize(triggerDataPoint, dataOutputSerializer);
		stream.write(dataOutputSerializer.getByteArray());
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		byte[] bytes = new byte[stream.available()];
		stream.readFully(bytes);
		triggerDataPoint = typeSerializer.deserialize(new DataInputDeserializer(bytes, 0, bytes.length));
	}
}
