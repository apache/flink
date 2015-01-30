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

package org.apache.flink.streaming.api.invokable.operator;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerCallback;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

public abstract class WindowInvokable<IN, OUT> extends StreamInvokable<IN, OUT> {

	/**
	 * Auto-generated serial version UID
	 */
	private static final long serialVersionUID = -8038984294071650730L;

	private LinkedList<TriggerPolicy<IN>> triggerPolicies;
	private LinkedList<EvictionPolicy<IN>> evictionPolicies;
	private LinkedList<ActiveTriggerPolicy<IN>> activeTriggerPolicies;
	private LinkedList<ActiveEvictionPolicy<IN>> activeEvictionPolicies;
	private LinkedList<Thread> activePolicyTreads;
	protected LinkedList<IN> buffer;
	private LinkedList<TriggerPolicy<IN>> currentTriggerPolicies;

	/**
	 * This constructor created a windowing invokable using trigger and eviction
	 * policies.
	 * 
	 * @param userFunction
	 *            The user defined {@link ReduceFunction}
	 * @param triggerPolicies
	 *            A list of {@link TriggerPolicy}s and/or
	 *            {@link ActiveTriggerPolicy}s
	 * @param evictionPolicies
	 *            A list of {@link EvictionPolicy}s and/or
	 *            {@link ActiveEvictionPolicy}s
	 */
	public WindowInvokable(Function userFunction, LinkedList<TriggerPolicy<IN>> triggerPolicies,
			LinkedList<EvictionPolicy<IN>> evictionPolicies) {
		super(userFunction);

		this.triggerPolicies = triggerPolicies;
		this.evictionPolicies = evictionPolicies;

		activeTriggerPolicies = new LinkedList<ActiveTriggerPolicy<IN>>();
		for (TriggerPolicy<IN> tp : triggerPolicies) {
			if (tp instanceof ActiveTriggerPolicy) {
				activeTriggerPolicies.add((ActiveTriggerPolicy<IN>) tp);
			}
		}

		activeEvictionPolicies = new LinkedList<ActiveEvictionPolicy<IN>>();
		for (EvictionPolicy<IN> ep : evictionPolicies) {
			if (ep instanceof ActiveEvictionPolicy) {
				activeEvictionPolicies.add((ActiveEvictionPolicy<IN>) ep);
			}
		}

		this.activePolicyTreads = new LinkedList<Thread>();
		this.buffer = new LinkedList<IN>();
		this.currentTriggerPolicies = new LinkedList<TriggerPolicy<IN>>();
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
		super.open(parameters);
		for (ActiveTriggerPolicy<IN> tp : activeTriggerPolicies) {
			Runnable target = tp.createActiveTriggerRunnable(new WindowingCallback(tp));
			if (target != null) {
				Thread thread = new Thread(target);
				activePolicyTreads.add(thread);
				thread.start();
			}
		}
	};

	/**
	 * This class allows the active trigger threads to call back and push fake
	 * elements at any time.
	 */
	private class WindowingCallback implements ActiveTriggerCallback {
		private ActiveTriggerPolicy<IN> policy;

		public WindowingCallback(ActiveTriggerPolicy<IN> policy) {
			this.policy = policy;
		}

		@Override
		public void sendFakeElement(Object datapoint) {
			processFakeElement(datapoint, this.policy);
		}

	}

	@Override
	public void invoke() throws Exception {

		// Prevent empty data streams
		if (readNext() == null) {
			throw new RuntimeException("DataStream must not be empty");
		}

		// Continuously run
		while (nextRecord != null) {
			processRealElement(nextRecord.getObject());

			// Load next StreamRecord
			readNext();
		}

		// Stop all remaining threads from policies
		for (Thread t : activePolicyTreads) {
			t.interrupt();
		}

		// finally trigger the buffer.
		emitFinalWindow(null);

	}

	/**
	 * This method gets called in case of an grouped windowing in case central
	 * trigger occurred and the arriving element causing the trigger is not part
	 * of this group.
	 * 
	 * Remark: This is NOT the same as
	 * {@link WindowInvokable#processFakeElement(Object, TriggerPolicy)}! Here
	 * the eviction using active policies takes place after the call to the UDF.
	 * Usually it is done before when fake elements get submitted. This special
	 * behaviour is needed to allow the {@link GroupedWindowInvokable} to send
	 * central triggers to all groups, even if the current element does not
	 * belong to the group.
	 * 
	 * @param input
	 *            a fake input element
	 * @param policies
	 *            the list of policies which caused the call with this fake
	 *            element
	 */
	protected synchronized void externalTriggerFakeElement(IN input,
			List<TriggerPolicy<IN>> policies) {

		// Set the current triggers
		currentTriggerPolicies.addAll(policies);

		// emit
		callUserFunctionAndLogException();

		// clear the flag collection
		currentTriggerPolicies.clear();

		// Process the evictions and take care of double evictions
		// In case there are multiple eviction policies present,
		// only the one with the highest return value is recognized.
		int currentMaxEviction = 0;
		for (ActiveEvictionPolicy<IN> evictionPolicy : activeEvictionPolicies) {
			// use temporary variable to prevent multiple calls to
			// notifyEviction
			int tmp = evictionPolicy.notifyEvictionWithFakeElement(input, buffer.size());
			if (tmp > currentMaxEviction) {
				currentMaxEviction = tmp;
			}
		}

		for (int i = 0; i < currentMaxEviction; i++) {
			try {
				buffer.removeFirst();
			} catch (NoSuchElementException e) {
				// In case no more elements are in the buffer:
				// Prevent failure and stop deleting.
				break;
			}
		}
	}

	/**
	 * This method processed an arrived fake element The method is synchronized
	 * to ensure that it cannot interleave with
	 * {@link WindowInvokable#processRealElement(Object)}
	 * 
	 * @param input
	 *            a fake input element
	 * @param currentPolicy
	 *            the policy which produced this fake element
	 */
	protected synchronized void processFakeElement(Object input, TriggerPolicy<IN> currentPolicy) {

		// Process the evictions and take care of double evictions
		// In case there are multiple eviction policies present,
		// only the one with the highest return value is recognized.
		int currentMaxEviction = 0;
		for (ActiveEvictionPolicy<IN> evictionPolicy : activeEvictionPolicies) {
			// use temporary variable to prevent multiple calls to
			// notifyEviction
			int tmp = evictionPolicy.notifyEvictionWithFakeElement(input, buffer.size());
			if (tmp > currentMaxEviction) {
				currentMaxEviction = tmp;
			}
		}

		for (int i = 0; i < currentMaxEviction; i++) {
			try {
				buffer.removeFirst();
			} catch (NoSuchElementException e) {
				// In case no more elements are in the buffer:
				// Prevent failure and stop deleting.
				break;
			}
		}

		// Set the current trigger
		currentTriggerPolicies.add(currentPolicy);

		// emit
		callUserFunctionAndLogException();

		// clear the flag collection
		currentTriggerPolicies.clear();
	}

	/**
	 * This method processed an arrived real element The method is synchronized
	 * to ensure that it cannot interleave with
	 * {@link WindowInvokable#processFakeElement(Object)}.
	 * 
	 * @param input
	 *            a real input element
	 * @param triggerPolicies
	 *            Allows to set trigger policies which are maintained
	 *            externally. This is the case for central policies in
	 *            {@link GroupedWindowInvokable}.
	 */
	protected synchronized void processRealElement(IN input, List<TriggerPolicy<IN>> triggerPolicies) {
		this.currentTriggerPolicies.addAll(triggerPolicies);
		processRealElement(input);
	}

	/**
	 * This method processed an arrived real element The method is synchronized
	 * to ensure that it cannot interleave with
	 * {@link WindowInvokable#processFakeElement(Object)}
	 * 
	 * @param input
	 *            a real input element
	 */
	protected synchronized void processRealElement(IN input) {

		// Run the precalls to detect missed windows
		for (ActiveTriggerPolicy<IN> trigger : activeTriggerPolicies) {
			// Remark: In case multiple active triggers are present the ordering
			// of the different fake elements returned by this triggers becomes
			// a problem. This might lead to unexpected results...
			// Should we limit the number of active triggers to 0 or 1?
			Object[] result = trigger.preNotifyTrigger(input);
			for (Object in : result) {
				processFakeElement(in, trigger);
			}
		}

		// Remember if a trigger occurred
		boolean isTriggered = false;

		// Process the triggers
		for (TriggerPolicy<IN> triggerPolicy : triggerPolicies) {
			if (triggerPolicy.notifyTrigger(input)) {
				currentTriggerPolicies.add(triggerPolicy);
			}
		}

		// call user function
		if (!currentTriggerPolicies.isEmpty()) {
			// emit
			callUserFunctionAndLogException();

			// clear the flag collection
			currentTriggerPolicies.clear();

			// remember trigger
			isTriggered = true;
		}

		// Process the evictions and take care of double evictions
		// In case there are multiple eviction policies present,
		// only the one with the highest return value is recognized.
		int currentMaxEviction = 0;

		for (EvictionPolicy<IN> evictionPolicy : evictionPolicies) {
			// use temporary variable to prevent multiple calls to
			// notifyEviction
			int tmp = evictionPolicy.notifyEviction(input, isTriggered, buffer.size());
			if (tmp > currentMaxEviction) {
				currentMaxEviction = tmp;
			}
		}

		for (int i = 0; i < currentMaxEviction; i++) {
			try {
				buffer.removeFirst();
			} catch (NoSuchElementException e) {
				// In case no more elements are in the buffer:
				// Prevent failure and stop deleting.
				break;
			}
		}

		// Add the current element to the buffer
		buffer.add(input);

	}

	/**
	 * This method removes the first element from the element buffer. It is used
	 * to provide central evictions in {@link GroupedWindowInvokable}
	 */
	protected synchronized void evictFirst() {
		try {
			buffer.removeFirst();
		} catch (NoSuchElementException e) {
			// ignore exception
		}
	}

	/**
	 * This method returns whether the element buffer is empty or not. It is
	 * used to figure out if a group can be deleted or not when
	 * {@link GroupedWindowInvokable} is used.
	 * 
	 * @return true in case the buffer is empty otherwise false.
	 */
	protected boolean isBufferEmpty() {
		return buffer.isEmpty();
	}

	/**
	 * This method does the final reduce at the end of the stream and emits the
	 * result.
	 * 
	 * @param centralTriggerPolicies
	 *            Allows to set trigger policies which are maintained
	 *            externally. This is the case for central policies in
	 *            {@link GroupedWindowInvokable}.
	 */
	protected void emitFinalWindow(List<TriggerPolicy<IN>> centralTriggerPolicies) {
		if (!buffer.isEmpty()) {
			currentTriggerPolicies.clear();

			if (centralTriggerPolicies != null) {
				currentTriggerPolicies.addAll(centralTriggerPolicies);
			}

			for (TriggerPolicy<IN> policy : triggerPolicies) {
				currentTriggerPolicies.add(policy);
			}

			callUserFunctionAndLogException();
		}
	}

}
