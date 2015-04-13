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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.policy.CentralActiveTrigger;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;

public class GroupedActiveDiscretizer<IN> extends GroupedStreamDiscretizer<IN> {

	private static final long serialVersionUID = -3469545957144404137L;

	private volatile IN last;
	private Thread centralThread;

	public GroupedActiveDiscretizer(KeySelector<IN, ?> keySelector,
			CentralActiveTrigger<IN> triggerPolicy, CloneableEvictionPolicy<IN> evictionPolicy) {
		super(keySelector, triggerPolicy, evictionPolicy);
	}

	@Override
	protected StreamDiscretizer<IN> makeNewGroup(Object key) throws Exception {

		StreamDiscretizer<IN> groupDiscretizer = new StreamDiscretizer<IN>(triggerPolicy.clone(),
				evictionPolicy.clone());

		groupDiscretizer.collector = taskContext.getOutputCollector();
		// We omit the groupDiscretizer.open(...) call here to avoid starting
		// new active threads
		return groupDiscretizer;
	}

	@Override
	public void run() throws Exception {

		while (isRunning && readNext() != null) {
			last = copy(nextObject);
			Object key = keySelector.getKey(nextObject);

			synchronized (groupedDiscretizers) {
				StreamDiscretizer<IN> groupDiscretizer = groupedDiscretizers.get(key);

				if (groupDiscretizer == null) {
					groupDiscretizer = makeNewGroup(key);
					groupedDiscretizers.put(key, groupDiscretizer);
				}

				groupDiscretizer.processRealElement(nextObject);
			}

		}

		for (StreamDiscretizer<IN> group : groupedDiscretizers.values()) {
			group.emitWindow();
		}

	}

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
		super.open(parameters);
		centralThread = new Thread(new CentralCheck());
		centralThread.start();
	}

	private class CentralCheck implements Runnable {

		@Override
		public void run() {
			while (isRunning) {
				// wait for the specified granularity
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// ignore it...
				}

				try {
					if (last != null) {
						synchronized (groupedDiscretizers) {
							for (StreamDiscretizer<IN> group : groupedDiscretizers.values()) {

								CentralActiveTrigger<IN> groupTrigger = (CentralActiveTrigger<IN>) group.triggerPolicy;
								Object[] fakes = groupTrigger.notifyOnLastGlobalElement(last);
								if (fakes != null) {
									for (Object fake : fakes) {
										group.triggerOnFakeElement(fake);
									}
								}
							}
						}

					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}

			}
		}
	}
}
