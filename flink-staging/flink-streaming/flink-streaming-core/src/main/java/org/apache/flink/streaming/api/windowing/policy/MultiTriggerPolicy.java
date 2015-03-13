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

import java.util.LinkedList;
import java.util.List;

/**
 * This class allows to use multiple trigger policies at the same time. It
 * allows to use both, active and not active triggers.
 * 
 * @param <DATA>
 *            the data type handled by this policy
 */
public class MultiTriggerPolicy<DATA> implements ActiveTriggerPolicy<DATA> {

	/**
	 * Default version id.
	 */
	private static final long serialVersionUID = 1L;

	private List<TriggerPolicy<DATA>> allTriggerPolicies;
	private List<ActiveTriggerPolicy<DATA>> activeTriggerPolicies;

	/**
	 * This policy allows to use multiple trigger policies at the same time. It
	 * allows to use both, active and not active triggers.
	 * 
	 * This policy triggers in case at least one of the nested policies
	 * triggered. If active policies are nested all produces fake elements will
	 * be returned.
	 * 
	 * @param policies
	 *            Any active or not active trigger policies. Both types can be
	 *            used at the same time.
	 */
	public MultiTriggerPolicy(TriggerPolicy<DATA>... policies) {
		allTriggerPolicies = new LinkedList<TriggerPolicy<DATA>>();
		activeTriggerPolicies = new LinkedList<ActiveTriggerPolicy<DATA>>();

		for (TriggerPolicy<DATA> policy : policies) {
			this.allTriggerPolicies.add(policy);
			if (policy instanceof ActiveTriggerPolicy) {
				this.activeTriggerPolicies.add((ActiveTriggerPolicy<DATA>) policy);
			}
		}
	}

	@Override
	public boolean notifyTrigger(DATA datapoint) {
		boolean trigger = false;
		for (TriggerPolicy<DATA> policy : allTriggerPolicies) {
			if (policy.notifyTrigger(datapoint)) {
				trigger = true;
				// Do not at a break here. All trigger must see the element!
			}
		}
		return trigger;
	}

	@Override
	public Object[] preNotifyTrigger(DATA datapoint) {
		List<Object> fakeElements = new LinkedList<Object>();
		for (ActiveTriggerPolicy<DATA> policy : activeTriggerPolicies) {
			for (Object fakeElement : policy.preNotifyTrigger(datapoint)) {
				fakeElements.add(fakeElement);
			}
		}
		return fakeElements.toArray();
	}

	@Override
	public Runnable createActiveTriggerRunnable(ActiveTriggerCallback callback) {
		List<Runnable> runnables = new LinkedList<Runnable>();
		for (ActiveTriggerPolicy<DATA> policy : activeTriggerPolicies) {
			Runnable tmp = policy.createActiveTriggerRunnable(callback);
			if (tmp != null) {
				runnables.add(tmp);
			}
		}
		if (runnables.size() == 0) {
			return null;
		} else {
			return new MultiActiveTriggerRunnable(runnables);
		}
	}

	/**
	 * This class serves a nest for all active trigger runnables. Once the run
	 * method gets executed, all the runnables are started in own threads.
	 */
	private class MultiActiveTriggerRunnable implements Runnable {

		List<Runnable> runnables;

		MultiActiveTriggerRunnable(List<Runnable> runnables) {
			this.runnables = runnables;
		}

		@Override
		public void run() {
			for (Runnable runnable : runnables) {
				new Thread(runnable).start();
			}
		}

	}
}
