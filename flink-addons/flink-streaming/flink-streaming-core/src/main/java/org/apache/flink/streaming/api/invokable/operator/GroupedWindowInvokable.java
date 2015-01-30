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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerCallback;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * This invokable allows windowing based on {@link TriggerPolicy} and
 * {@link EvictionPolicy} instances including their active and cloneable
 * versions. It is additionally aware of the creation of windows per group.
 * 
 * A {@link KeySelector} is used to specify the key position or key extraction.
 * The {@link ReduceFunction} will be executed on each group separately.
 * Policies might either be centralized or distributed. It is not possible to
 * use central and distributed eviction policies at the same time. A distributed
 * policy have to be a {@link CloneableTriggerPolicy} or
 * {@link CloneableEvictionPolicy} as it will be cloned to have separated
 * instances for each group. At the startup time the distributed policies will
 * be stored as sample, and only clones of them will be used to maintain the
 * groups. Therefore, each group starts with the initial policy states.
 * 
 * While a distributed policy only gets notified with the elements belonging to
 * the respective group, a centralized policy get notified with all arriving
 * elements. When a centralized trigger occurred, all groups get triggered. This
 * is done by submitting the element which caused the trigger as real element to
 * the groups it belongs to and as fake element to all other groups. Within the
 * groups the element might be further processed, causing more triggers,
 * prenotifications of active distributed policies and evictions like usual.
 * 
 * Central policies can be instance of {@link ActiveTriggerPolicy} and also
 * implement the
 * {@link ActiveTriggerPolicy#createActiveTriggerRunnable(ActiveTriggerCallback)}
 * method. Fake elements created on prenotification will be forwarded to all
 * groups. The {@link ActiveTriggerCallback} is also implemented in a way, that
 * it forwards/distributed calls all groups.
 * 
 * @param <IN>
 *            The type of input elements handled by this operator invokable.
 */
public class GroupedWindowInvokable<IN, OUT> extends StreamInvokable<IN, OUT> {

	/**
	 * Auto-generated serial version UID
	 */
	private static final long serialVersionUID = -3469545957144404137L;

	private KeySelector<IN, ?> keySelector;
	private Configuration parameters;
	private LinkedList<ActiveTriggerPolicy<IN>> activeCentralTriggerPolicies;
	private LinkedList<TriggerPolicy<IN>> centralTriggerPolicies;
	private LinkedList<ActiveEvictionPolicy<IN>> activeCentralEvictionPolicies;
	private LinkedList<EvictionPolicy<IN>> centralEvictionPolicies;
	private LinkedList<CloneableTriggerPolicy<IN>> distributedTriggerPolicies;
	private LinkedList<CloneableEvictionPolicy<IN>> distributedEvictionPolicies;
	private Map<Object, WindowInvokable<IN, OUT>> windowingGroups;
	private LinkedList<Thread> activePolicyThreads;
	private LinkedList<TriggerPolicy<IN>> currentTriggerPolicies;
	private LinkedList<WindowInvokable<IN, OUT>> deleteOrderForCentralEviction;

	/**
	 * This constructor creates an instance of the grouped windowing invokable.
	 * 
	 * A {@link KeySelector} is used to specify the key position or key
	 * extraction. The {@link ReduceFunction} will be executed on each group
	 * separately. Policies might either be centralized or distributed. It is
	 * not possible to use central and distributed eviction policies at the same
	 * time. A distributed policy have to be a {@link CloneableTriggerPolicy} or
	 * {@link CloneableEvictionPolicy} as it will be cloned to have separated
	 * instances for each group. At the startup time the distributed policies
	 * will be stored as sample, and only clones of them will be used to
	 * maintain the groups. Therefore, each group starts with the initial policy
	 * states.
	 * 
	 * While a distributed policy only gets notified with the elements belonging
	 * to the respective group, a centralized policy get notified with all
	 * arriving elements. When a centralized trigger occurred, all groups get
	 * triggered. This is done by submitting the element which caused the
	 * trigger as real element to the groups it belongs to and as fake element
	 * to all other groups. Within the groups the element might be further
	 * processed, causing more triggers, prenotifications of active distributed
	 * policies and evictions like usual.
	 * 
	 * Central policies can be instance of {@link ActiveTriggerPolicy} and also
	 * implement the
	 * {@link ActiveTriggerPolicy#createActiveTriggerRunnable(ActiveTriggerCallback)}
	 * method. Fake elements created on prenotification will be forwarded to all
	 * groups. The {@link ActiveTriggerCallback} is also implemented in a way,
	 * that it forwards/distributed calls all groups.
	 * 
	 * @param userFunction
	 *            The user defined function.
	 * @param keySelector
	 *            A key selector to extract the key for the groups from the
	 *            input data.
	 * @param distributedTriggerPolicies
	 *            Trigger policies to be distributed and maintained individually
	 *            within each group.
	 * @param distributedEvictionPolicies
	 *            Eviction policies to be distributed and maintained
	 *            individually within each group. Note that there cannot be
	 *            both, central and distributed eviction policies at the same
	 *            time.
	 * @param centralTriggerPolicies
	 *            Trigger policies which will only exist once at a central
	 *            place. In case a central policy triggers, it will cause all
	 *            groups to be emitted. (Remark: Empty groups cannot be emitted.
	 *            If only one element is contained a group, this element itself
	 *            is returned as aggregated result.)
	 * @param centralEvictionPolicies
	 *            Eviction which will only exist once at a central place. Note
	 *            that there cannot be both, central and distributed eviction
	 *            policies at the same time. The central eviction policy will
	 *            work on an simulated element buffer containing all elements no
	 *            matter which group they belong to.
	 */
	public GroupedWindowInvokable(Function userFunction, KeySelector<IN, ?> keySelector,
			LinkedList<CloneableTriggerPolicy<IN>> distributedTriggerPolicies,
			LinkedList<CloneableEvictionPolicy<IN>> distributedEvictionPolicies,
			LinkedList<TriggerPolicy<IN>> centralTriggerPolicies,
			LinkedList<EvictionPolicy<IN>> centralEvictionPolicies) {

		super(userFunction);

		this.keySelector = keySelector;

		// handle the triggers
		if (centralTriggerPolicies != null) {
			this.centralTriggerPolicies = centralTriggerPolicies;
			this.activeCentralTriggerPolicies = new LinkedList<ActiveTriggerPolicy<IN>>();

			for (TriggerPolicy<IN> trigger : centralTriggerPolicies) {
				if (trigger instanceof ActiveTriggerPolicy) {
					this.activeCentralTriggerPolicies.add((ActiveTriggerPolicy<IN>) trigger);
				}
			}
		} else {
			this.centralTriggerPolicies = new LinkedList<TriggerPolicy<IN>>();
		}

		if (distributedTriggerPolicies != null) {
			this.distributedTriggerPolicies = distributedTriggerPolicies;
		} else {
			this.distributedTriggerPolicies = new LinkedList<CloneableTriggerPolicy<IN>>();
		}

		if (distributedEvictionPolicies != null) {
			this.distributedEvictionPolicies = distributedEvictionPolicies;
		} else {
			this.distributedEvictionPolicies = new LinkedList<CloneableEvictionPolicy<IN>>();
		}

		this.activeCentralEvictionPolicies = new LinkedList<ActiveEvictionPolicy<IN>>();

		if (centralEvictionPolicies != null) {
			this.centralEvictionPolicies = centralEvictionPolicies;

			for (EvictionPolicy<IN> eviction : centralEvictionPolicies) {
				if (eviction instanceof ActiveEvictionPolicy) {
					this.activeCentralEvictionPolicies.add((ActiveEvictionPolicy<IN>) eviction);
				}
			}
		} else {
			this.centralEvictionPolicies = new LinkedList<EvictionPolicy<IN>>();
		}

		this.windowingGroups = new HashMap<Object, WindowInvokable<IN, OUT>>();
		this.activePolicyThreads = new LinkedList<Thread>();
		this.currentTriggerPolicies = new LinkedList<TriggerPolicy<IN>>();
		this.deleteOrderForCentralEviction = new LinkedList<WindowInvokable<IN, OUT>>();

		// check that not both, central and distributed eviction, is used at the
		// same time.
		if (!this.centralEvictionPolicies.isEmpty() && !this.distributedEvictionPolicies.isEmpty()) {
			throw new UnsupportedOperationException(
					"You can only use either central or distributed eviction policies but not both at the same time.");
		}

		// Check that there is at least one trigger and one eviction policy
		if (this.centralEvictionPolicies.isEmpty() && this.distributedEvictionPolicies.isEmpty()) {
			throw new UnsupportedOperationException(
					"You have to define at least one eviction policy");
		}
		if (this.centralTriggerPolicies.isEmpty() && this.distributedTriggerPolicies.isEmpty()) {
			throw new UnsupportedOperationException(
					"You have to define at least one trigger policy");
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
			WindowInvokable<IN, OUT> groupInvokable = windowingGroups.get(keySelector
					.getKey(nextRecord.getObject()));
			if (groupInvokable == null) {
				groupInvokable = makeNewGroup(nextRecord);
			}

			// Run the precalls for central active triggers
			for (ActiveTriggerPolicy<IN> trigger : activeCentralTriggerPolicies) {
				Object[] result = trigger.preNotifyTrigger(nextRecord.getObject());
				for (Object in : result) {

					// If central eviction is used, handle it here
					if (!activeCentralEvictionPolicies.isEmpty()) {
						evictElements(centralActiveEviction(in));
					}

					// process in groups
					for (WindowInvokable<IN, OUT> group : windowingGroups.values()) {
						group.processFakeElement(in, trigger);
						checkForEmptyGroupBuffer(group);
					}
				}
			}

			// Process non-active central triggers
			for (TriggerPolicy<IN> triggerPolicy : centralTriggerPolicies) {
				if (triggerPolicy.notifyTrigger(nextRecord.getObject())) {
					currentTriggerPolicies.add(triggerPolicy);
				}
			}

			if (currentTriggerPolicies.isEmpty()) {

				// only add the element to its group
				groupInvokable.processRealElement(nextRecord.getObject());
				checkForEmptyGroupBuffer(groupInvokable);

				// If central eviction is used, handle it here
				if (!centralEvictionPolicies.isEmpty()) {
					evictElements(centralEviction(nextRecord.getObject(), false));
					deleteOrderForCentralEviction.add(groupInvokable);
				}

			} else {

				// call user function for all groups
				for (WindowInvokable<IN, OUT> group : windowingGroups.values()) {
					if (group == groupInvokable) {
						// process real with initialized policies
						group.processRealElement(nextRecord.getObject(), currentTriggerPolicies);
					} else {
						// process like a fake but also initialized with
						// policies
						group.externalTriggerFakeElement(nextRecord.getObject(),
								currentTriggerPolicies);
					}

					// remove group in case it has an empty buffer
					// checkForEmptyGroupBuffer(group);
				}

				// If central eviction is used, handle it here
				if (!centralEvictionPolicies.isEmpty()) {
					evictElements(centralEviction(nextRecord.getObject(), true));
					deleteOrderForCentralEviction.add(groupInvokable);
				}
			}

			// clear current trigger list
			currentTriggerPolicies.clear();

			// read next record
			readNext();
		}

		// Stop all remaining threads from policies
		for (Thread t : activePolicyThreads) {
			t.interrupt();
		}

		// finally trigger the buffer.
		for (WindowInvokable<IN, OUT> group : windowingGroups.values()) {
			group.emitFinalWindow(centralTriggerPolicies);
		}

	}

	/**
	 * This method creates a new group. The method gets called in case an
	 * element arrives which has a key which was not seen before. The method
	 * created a nested {@link WindowInvokable} and therefore created clones of
	 * all distributed trigger and eviction policies.
	 * 
	 * @param element
	 *            The element which leads to the generation of a new group
	 *            (previously unseen key)
	 * @throws Exception
	 *             In case the {@link KeySelector} throws an exception in
	 *             {@link KeySelector#getKey(Object)}, the exception is not
	 *             catched by this method.
	 */
	@SuppressWarnings("unchecked")
	private WindowInvokable<IN, OUT> makeNewGroup(StreamRecord<IN> element) throws Exception {
		// clone the policies
		LinkedList<TriggerPolicy<IN>> clonedDistributedTriggerPolicies = new LinkedList<TriggerPolicy<IN>>();
		LinkedList<EvictionPolicy<IN>> clonedDistributedEvictionPolicies = new LinkedList<EvictionPolicy<IN>>();
		for (CloneableTriggerPolicy<IN> trigger : this.distributedTriggerPolicies) {
			clonedDistributedTriggerPolicies.add(trigger.clone());
		}
		for (CloneableEvictionPolicy<IN> eviction : this.distributedEvictionPolicies) {
			clonedDistributedEvictionPolicies.add(eviction.clone());
		}

		WindowInvokable<IN, OUT> groupInvokable;
		if (userFunction instanceof ReduceFunction) {
			groupInvokable = (WindowInvokable<IN, OUT>) new WindowReduceInvokable<IN>(
					(ReduceFunction<IN>) userFunction, clonedDistributedTriggerPolicies,
					clonedDistributedEvictionPolicies);
		} else {
			groupInvokable = new WindowGroupReduceInvokable<IN, OUT>(
					(GroupReduceFunction<IN, OUT>) userFunction, clonedDistributedTriggerPolicies,
					clonedDistributedEvictionPolicies);
		}

		groupInvokable.setup(taskContext);
		groupInvokable.open(this.parameters);
		windowingGroups.put(keySelector.getKey(element.getObject()), groupInvokable);

		return groupInvokable;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.parameters = parameters;
		for (ActiveTriggerPolicy<IN> tp : activeCentralTriggerPolicies) {
			Runnable target = tp.createActiveTriggerRunnable(new WindowingCallback(tp));
			if (target != null) {
				Thread thread = new Thread(target);
				activePolicyThreads.add(thread);
				thread.start();
			}
		}
	};

	/**
	 * This method is used to notify central eviction policies with a real
	 * element.
	 * 
	 * @param input
	 *            the real element to notify the eviction policy.
	 * @param triggered
	 *            whether a central trigger occurred or not.
	 * @return The number of elements to be deleted from the buffer.
	 */
	private int centralEviction(IN input, boolean triggered) {
		// Process the evictions and take care of double evictions
		// In case there are multiple eviction policies present,
		// only the one with the highest return value is recognized.
		int currentMaxEviction = 0;
		for (EvictionPolicy<IN> evictionPolicy : centralEvictionPolicies) {
			// use temporary variable to prevent multiple calls to
			// notifyEviction
			int tmp = evictionPolicy.notifyEviction(input, triggered,
					deleteOrderForCentralEviction.size());
			if (tmp > currentMaxEviction) {
				currentMaxEviction = tmp;
			}
		}
		return currentMaxEviction;
	}

	/**
	 * This method is used to notify active central eviction policies with a
	 * fake element.
	 * 
	 * @param input
	 *            the fake element to notify the active central eviction
	 *            policies.
	 * @return The number of elements to be deleted from the buffer.
	 */
	private int centralActiveEviction(Object input) {
		// Process the evictions and take care of double evictions
		// In case there are multiple eviction policies present,
		// only the one with the highest return value is recognized.
		int currentMaxEviction = 0;
		for (ActiveEvictionPolicy<IN> evictionPolicy : activeCentralEvictionPolicies) {
			// use temporary variable to prevent multiple calls to
			// notifyEviction
			int tmp = evictionPolicy.notifyEvictionWithFakeElement(input,
					deleteOrderForCentralEviction.size());
			if (tmp > currentMaxEviction) {
				currentMaxEviction = tmp;
			}
		}
		return currentMaxEviction;
	}

	/**
	 * This method is used in central eviction to delete a given number of
	 * elements from the buffer.
	 * 
	 * @param numToEvict
	 *            number of elements to delete from the virtual central element
	 *            buffer.
	 */
	private void evictElements(int numToEvict) {
		HashSet<WindowInvokable<IN, OUT>> usedGroups = new HashSet<WindowInvokable<IN, OUT>>();
		for (; numToEvict > 0; numToEvict--) {
			WindowInvokable<IN, OUT> currentGroup = deleteOrderForCentralEviction.getFirst();
			// Do the eviction
			currentGroup.evictFirst();
			// Remember groups which possibly have an empty buffer after the
			// eviction
			usedGroups.add(currentGroup);
			try {
				deleteOrderForCentralEviction.removeFirst();
			} catch (NoSuchElementException e) {
				// when buffer is empty, ignore exception and stop deleting
				break;
			}

		}

		// Remove groups with empty buffer
		for (WindowInvokable<IN, OUT> group : usedGroups) {
			checkForEmptyGroupBuffer(group);
		}
	}

	/**
	 * Checks if the element buffer of a given windowing group is empty. If so,
	 * the group will be deleted.
	 * 
	 * @param group
	 *            The windowing group to be checked and and removed in case its
	 *            buffer is empty.
	 */
	private void checkForEmptyGroupBuffer(WindowInvokable<IN, OUT> group) {
		if (group.isBufferEmpty()) {
			windowingGroups.remove(group);
		}
	}

	/**
	 * This callback class allows to handle the callbacks done by threads
	 * defined in active trigger policies
	 * 
	 * @see ActiveTriggerPolicy#createActiveTriggerRunnable(ActiveTriggerCallback)
	 */
	private class WindowingCallback implements ActiveTriggerCallback {
		private ActiveTriggerPolicy<IN> policy;

		public WindowingCallback(ActiveTriggerPolicy<IN> policy) {
			this.policy = policy;
		}

		@Override
		public void sendFakeElement(Object datapoint) {

			// If central eviction is used, handle it here
			if (!centralEvictionPolicies.isEmpty()) {
				evictElements(centralActiveEviction(datapoint));
			}

			// handle element in groups
			for (WindowInvokable<IN, OUT> group : windowingGroups.values()) {
				group.processFakeElement(datapoint, policy);
				checkForEmptyGroupBuffer(group);
			}
		}

	}
}
