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

package org.apache.flink.streaming.api.windowing.helper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
import org.apache.flink.streaming.api.windowing.policy.DeltaPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * This helper represents a trigger or eviction policy based on a
 * {@link DeltaFunction}.
 * 
 * @param <DATA>
 *            the data type handled by the delta function represented by this
 *            helper.
 */
public class Delta<DATA> extends WindowingHelper<DATA> {

	private DeltaFunction<DATA> deltaFunction;
	private DATA initVal;
	private double threshold;
	private TypeSerializer<DATA> typeSerializer;

	/**
	 * Creates a delta helper representing a delta count or eviction policy
	 * @param deltaFunction
	 *				The delta function which should be used to calculate the delta
	 *				points.
	 * @param initVal
	 *				The initial value which will be used to calculate the first
	 *				delta.
	 * @param threshold
	 * 				The threshold used by the delta function.
	 */
	public Delta(DeltaFunction<DATA> deltaFunction, DATA initVal, double threshold) {
		this.deltaFunction = deltaFunction;
		this.initVal = initVal;
		this.threshold = threshold;
	}

	@Override
	public EvictionPolicy<DATA> toEvict() {
		instantiateTypeSerializer();
		return new DeltaPolicy<DATA>(deltaFunction, initVal, threshold, typeSerializer);
	}

	@Override
	public TriggerPolicy<DATA> toTrigger() {
		instantiateTypeSerializer();
		return new DeltaPolicy<DATA>(deltaFunction, initVal, threshold, typeSerializer);
	}

	/**
	 * Creates a delta helper representing a delta trigger or eviction policy.
	 * </br></br> This policy calculates a delta between the data point which
	 * triggered last and the currently arrived data point. It triggers if the
	 * delta is higher than a specified threshold. </br></br> In case it gets
	 * used for eviction, this policy starts from the first element of the
	 * buffer and removes all elements from the buffer which have a higher delta
	 * then the threshold. As soon as there is an element with a lower delta,
	 * the eviction stops.
	 *
	 * @param deltaFunction
	 *				The delta function which should be used to calculate the delta
	 *				points.
	 * @param initVal
	 *				The initial value which will be used to calculate the first
	 *				delta.
	 * @param threshold
	 * 				The threshold used by the delta function.
	 * @return Helper representing a delta trigger or eviction policy
	 */
	public static <DATA> Delta<DATA> of(double threshold, DeltaFunction<DATA> deltaFunction,
			DATA initVal) {
		return new Delta<DATA>(deltaFunction, initVal, threshold);
	}

	private void instantiateTypeSerializer(){
		if (executionConfig == null){
			throw new UnsupportedOperationException("ExecutionConfig has to be set to instantiate TypeSerializer.");
		}
		TypeInformation typeInformation = TypeExtractor.getForObject(initVal);
		typeSerializer = typeInformation.createSerializer(executionConfig);
	}
}
