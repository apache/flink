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

package org.apache.flink.streaming.api.operators.windowing;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;

/**
 * This operator represents the grouped discretization step of a window
 * transformation. The user supplied eviction and trigger policies are applied
 * on a per group basis to create the {@link StreamWindow} that will be further
 * transformed in the next stages. </p> To allow pre-aggregations supply an
 * appropriate {@link WindowBuffer}
 */
public class GroupedStreamDiscretizer<IN> extends StreamDiscretizer<IN> {

	private static final long serialVersionUID = 1L;

	protected KeySelector<IN, ?> keySelector;
	protected Configuration parameters;
	protected CloneableTriggerPolicy<IN> triggerPolicy;
	protected CloneableEvictionPolicy<IN> evictionPolicy;

	protected Map<Object, StreamDiscretizer<IN>> groupedDiscretizers;

	public GroupedStreamDiscretizer(KeySelector<IN, ?> keySelector,
			CloneableTriggerPolicy<IN> triggerPolicy, CloneableEvictionPolicy<IN> evictionPolicy) {

		super(triggerPolicy, evictionPolicy);

		this.keySelector = keySelector;

		this.triggerPolicy = triggerPolicy;
		this.evictionPolicy = evictionPolicy;

		this.groupedDiscretizers = new HashMap<Object, StreamDiscretizer<IN>>();
	}

	@Override
	public void close() throws Exception {
		super.close();
		for (StreamDiscretizer<IN> group : groupedDiscretizers.values()) {
			group.emitWindow();
		}
	}

	@Override
	public void processElement(IN element) throws Exception {


			Object key = keySelector.getKey(element);

			StreamDiscretizer<IN> groupDiscretizer = groupedDiscretizers.get(key);

			if (groupDiscretizer == null) {
				groupDiscretizer = makeNewGroup(key);
				groupedDiscretizers.put(key, groupDiscretizer);
			}

			groupDiscretizer.processRealElement(element);

	}

	/**
	 * This method creates a new group. The method gets called in case an
	 * element arrives which has a key which was not seen before. The method
	 * created a nested {@link StreamDiscretizer} and therefore created clones
	 * of all distributed trigger and eviction policies.
	 * 
	 * @param key
	 *            The key of the new group.
	 */
	protected StreamDiscretizer<IN> makeNewGroup(Object key) throws Exception {

		StreamDiscretizer<IN> groupDiscretizer = new StreamDiscretizer<IN>(triggerPolicy.clone(),
				evictionPolicy.clone());

//		groupDiscretizer.output = taskContext.getOutputCollector();
		// TODO: this seems very hacky, maybe we can get around this
		groupDiscretizer.setup(this.output, this.runtimeContext);
		groupDiscretizer.open(this.parameters);


		return groupDiscretizer;
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof GroupedStreamDiscretizer)) {
			return false;
		} else {
			try {
				@SuppressWarnings("unchecked")
				GroupedStreamDiscretizer<IN> otherDiscretizer = (GroupedStreamDiscretizer<IN>) other;

				return triggerPolicy.equals(otherDiscretizer.triggerPolicy)
						&& evictionPolicy.equals(otherDiscretizer.evictionPolicy)
						&& keySelector.equals(otherDiscretizer.keySelector);

			} catch (ClassCastException e) {
				return false;
			}
		}
	}

	@Override
	public String toString() {
		return "GroupedDiscretizer(Key: " + keySelector.getClass().getSimpleName() + ", Trigger: "
				+ triggerPolicy.toString() + ", Eviction: " + evictionPolicy.toString() + ")";
	}
}
