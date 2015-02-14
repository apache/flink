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

package org.apache.flink.streaming.api.invokable.operator.windowing;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;

/**
 * A specialized {@link GroupedStreamDiscretizer} to be used with time only
 * policies
 */
public class GroupedTimeDiscretizer<IN> extends GroupedStreamDiscretizer<IN> {

	private static final long serialVersionUID = -3469545957144404137L;

	private TimeTriggerPolicy<IN> timeTriggerPolicy;
	private Thread policyThread;

	public GroupedTimeDiscretizer(KeySelector<IN, ?> keySelector,
			TimeTriggerPolicy<IN> triggerPolicy, CloneableEvictionPolicy<IN> evictionPolicy,
			WindowBuffer<IN> windowBuffer) {

		super(keySelector, triggerPolicy, evictionPolicy, windowBuffer);
		this.timeTriggerPolicy = triggerPolicy;
	}

	@Override
	protected StreamDiscretizer<IN> makeNewGroup(Object key) throws Exception {

		StreamDiscretizer<IN> groupDiscretizer = new StreamDiscretizer<IN>(triggerPolicy.clone(),
				evictionPolicy.clone(), windowBuffer.clone());

		groupDiscretizer.collector = taskContext.getOutputCollector();
		// We omit the groupDiscretizer.open(...) call here to avoid starting
		// new active threads
		return groupDiscretizer;
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
		super.open(parameters);

		Runnable runnable = new TimeCheck();
		policyThread = new Thread(runnable);
		policyThread.start();
	}

	private class TimeCheck implements Runnable {

		@Override
		public void run() {
			while (true) {
				// wait for the specified granularity
				try {
					Thread.sleep(timeTriggerPolicy.granularity);
				} catch (InterruptedException e) {
					// ignore it...
				}

				for (StreamDiscretizer<IN> group : groupedDiscretizers.values()) {
					TimeTriggerPolicy<IN> groupTrigger = (TimeTriggerPolicy<IN>) group.triggerPolicy;
					Object fake = groupTrigger.activeFakeElementEmission(null);
					if (fake != null) {
						group.triggerOnFakeElement(fake);
					}
				}
			}
		}
	}
}
