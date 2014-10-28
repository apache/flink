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

package org.apache.flink.streaming.api.datastream;

import java.util.LinkedList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.operator.WindowingInvokable;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;
import org.apache.flink.streaming.util.serialization.CombineTypeWrapper;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;
import org.apache.flink.streaming.util.serialization.ObjectTypeWrapper;

/**
 * A {@link WindowedDataStream} represents a data stream whose elements
 * are batched together in a sliding batch. operations like
 * {@link #reduce(ReduceFunction)} or {@link #reduceGroup(GroupReduceFunction)}
 * are applied for each batch and the batch is slid afterwards.
 *
 * @param <OUT>
 *            The output type of the {@link WindowedDataStream}
 */
public class WindowedDataStream<OUT> {

	protected DataStream<OUT> dataStream;
	protected boolean isGrouped;
	protected KeySelector<OUT, ?> keySelector;

	protected WindowingHelper<OUT>[] triggerPolicies;
	protected WindowingHelper<OUT>[] evictionPolicies;

	protected WindowedDataStream(DataStream<OUT> dataStream,
			WindowingHelper<OUT>... policyHelpers) {
		if (dataStream instanceof GroupedDataStream) {
			this.isGrouped = true;
			this.keySelector = ((GroupedDataStream<OUT>) dataStream).keySelector;
		} else {
			this.isGrouped = false;
		}
		this.dataStream = dataStream.copy();
		this.triggerPolicies = policyHelpers;
	}

	protected LinkedList<TriggerPolicy<OUT>> getTriggers() {
		LinkedList<TriggerPolicy<OUT>> triggerPolicyList = new LinkedList<TriggerPolicy<OUT>>();

		for (WindowingHelper<OUT> helper : triggerPolicies) {
			triggerPolicyList.add(helper.toTrigger());
		}

		return triggerPolicyList;
	}

	protected LinkedList<EvictionPolicy<OUT>> getEvicters() {
		LinkedList<EvictionPolicy<OUT>> evictionPolicyList = new LinkedList<EvictionPolicy<OUT>>();

		if (evictionPolicies != null) {
			for (WindowingHelper<OUT> helper : evictionPolicies) {
				evictionPolicyList.add(helper.toEvict());
			}
		} else {
			evictionPolicyList.add(new TumblingEvictionPolicy<OUT>());
		}

		return evictionPolicyList;
	}

	protected WindowedDataStream(WindowedDataStream<OUT> windowedDataStream) {
		this.dataStream = windowedDataStream.dataStream.copy();
		this.isGrouped = windowedDataStream.isGrouped;
		this.keySelector = windowedDataStream.keySelector;
		this.triggerPolicies = windowedDataStream.triggerPolicies;
		this.evictionPolicies = windowedDataStream.evictionPolicies;

	}

	/**
	 * Groups the elements of the {@link WindowedDataStream} by the given
	 * key position to be used with grouped operators.
	 * 
	 * @param keySelector
	 *            The specification of the key on which the
	 *            {@link WindowedDataStream} will be grouped.
	 * @return The transformed {@link WindowedDataStream}
	 */
	public WindowedDataStream<OUT> groupBy(KeySelector<OUT, ?> keySelector) {
		WindowedDataStream<OUT> ret = this.copy();
		ret.dataStream = ret.dataStream.groupBy(keySelector);
		ret.isGrouped = true;
		ret.keySelector = keySelector;
		return ret;
	}

	/**
	 * This is a prototype implementation for new windowing features based on
	 * trigger and eviction policies
	 * 
	 * @param triggerPolicies
	 *            A list of trigger policies
	 * @param evictionPolicies
	 *            A list of eviction policies
	 * @param sample
	 *            A sample of the OUT data type required to gather type
	 *            information
	 * @return The single output operator
	 */
	public SingleOutputStreamOperator<Tuple2<OUT, String[]>, ?> reduce(
			ReduceFunction<OUT> reduceFunction) {
		return dataStream.addFunction("NextGenWindowReduce", reduceFunction,
				new FunctionTypeWrapper<OUT>(reduceFunction, ReduceFunction.class, 0),
				new CombineTypeWrapper<OUT, String[]>(dataStream.outTypeWrapper,
						new ObjectTypeWrapper<String[]>(new String[] { "" })),
				new WindowingInvokable<OUT>(reduceFunction, getTriggers(), getEvicters()));
	}

	/**
	 * Gets the output type.
	 * 
	 * @return The output type.
	 */
	public TypeInformation<OUT> getOutputType() {
		return dataStream.getOutputType();
	}

	protected WindowedDataStream<OUT> copy() {
		return new WindowedDataStream<OUT>(this);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public WindowedDataStream<OUT> every(WindowingHelper... policyHelpers) {
		WindowedDataStream<OUT> ret = this.copy();
		if (ret.evictionPolicies == null) {
			ret.evictionPolicies = ret.triggerPolicies;
			ret.triggerPolicies = policyHelpers;
		} else {
			ret.triggerPolicies = (WindowingHelper<OUT>[]) ArrayUtils.addAll(triggerPolicies,
					policyHelpers);
		}
		return ret;
	}
}
