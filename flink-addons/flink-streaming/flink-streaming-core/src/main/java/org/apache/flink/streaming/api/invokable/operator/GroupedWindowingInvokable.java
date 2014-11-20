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
import java.util.LinkedList;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerCallback;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This invokable allows windowing based on {@link TriggerPolicy} and
 * {@link EvictionPolicy} instances including their active and cloneable
 * versions. It is additionally aware of the creation of windows per group.
 * 
 * A {@link KeySelector} is used to specify the key position or key extraction.
 * The {@link ReduceFunction} will be executed on each group separately. Trigger
 * policies might either be centralized or distributed. Eviction policies are
 * always distributed. A distributed policy have to be a
 * {@link CloneableTriggerPolicy} or {@link CloneableEvictionPolicy} as it will
 * be cloned to have separated instances for each group. At the startup time the
 * distributed policies will be stored as sample, and only clones of them will
 * be used to maintain the groups. Therefore, each group starts with the initial
 * policy states.
 * 
 * While a distributed policy only gets notified with the elements belonging to
 * the respective group, a centralized policy get notified with all arriving
 * elements. When a centralized trigger occurred, all groups get triggered. This
 * is done by submitting the element which caused the trigger as real element to
 * the groups it belongs to and as fake element to all other groups. Within the
 * groups the element might be further processed, causing more triggered,
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
public class GroupedWindowingInvokable<IN> extends StreamInvokable<IN, Tuple2<IN, String[]>> {

	/**
	 * Auto-generated serial version UID
	 */
	private static final long serialVersionUID = -3469545957144404137L;

	private static final Logger LOG = LoggerFactory.getLogger(GroupedWindowingInvokable.class);

	private KeySelector<IN, ?> keySelector;
	private Configuration parameters;
	private LinkedList<ActiveTriggerPolicy<IN>> activeCentralTriggerPolicies = new LinkedList<ActiveTriggerPolicy<IN>>();
	private LinkedList<TriggerPolicy<IN>> centralTriggerPolicies = new LinkedList<TriggerPolicy<IN>>();
	private LinkedList<CloneableTriggerPolicy<IN>> distributedTriggerPolicies = new LinkedList<CloneableTriggerPolicy<IN>>();
	private LinkedList<CloneableEvictionPolicy<IN>> distributedEvictionPolicies = new LinkedList<CloneableEvictionPolicy<IN>>();
	private Map<Object, WindowingInvokable<IN>> windowingGroups = new HashMap<Object, WindowingInvokable<IN>>();
	private LinkedList<Thread> activePolicyThreads = new LinkedList<Thread>();
	private LinkedList<TriggerPolicy<IN>> currentTriggerPolicies = new LinkedList<TriggerPolicy<IN>>();

	/**
	 * This constructor creates an instance of the grouped windowing invokable.
	 * A {@link KeySelector} is used to specify the key position or key
	 * extraction. The {@link ReduceFunction} will be executed on each group
	 * separately. Trigger policies might either be centralized or distributed.
	 * Eviction policies are always distributed. A distributed policy have to be
	 * a {@link CloneableTriggerPolicy} or {@link CloneableEvictionPolicy} as it
	 * will be cloned to have separated instances for each group. At the startup
	 * time the distributed policies will be stored as sample, and only clones
	 * of them will be used to maintain the groups. Therefore, each group starts
	 * with the initial policy states.
	 * 
	 * While a distributed policy only gets notified with the elements belonging
	 * to the respective group, a centralized policy get notified with all
	 * arriving elements. When a centralized trigger occurred, all groups get
	 * triggered. This is done by submitting the element which caused the
	 * trigger as real element to the groups it belongs to and as fake element
	 * to all other groups. Within the groups the element might be further
	 * processed, causing more triggered, prenotifications of active distributed
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
	 *            The user defined {@link ReduceFunction}.
	 * @param keySelector
	 *            A key selector to extract the key for the groups from the
	 *            input data.
	 * @param distributedTriggerPolicies
	 *            Trigger policies to be distributed and maintained individually
	 *            within each group.
	 * @param distributedEvictionPolicies
	 *            Eviction policies to be distributed and maintained
	 *            individually within each group. There are no central eviction
	 *            policies because there is no central element buffer but only a
	 *            buffer per group. Therefore evictions might always be done per
	 *            group.
	 * @param centralTriggerPolicies
	 *            Trigger policies which will only exist once at a central
	 *            place. In case a central policy triggers, it will cause all
	 *            groups to be emitted. (Remark: Empty groups cannot be emitted.
	 *            If only one element is contained a group, this element itself
	 *            is returned as aggregated result.)
	 */
	public GroupedWindowingInvokable(ReduceFunction<IN> userFunction,
			KeySelector<IN, ?> keySelector,
			LinkedList<CloneableTriggerPolicy<IN>> distributedTriggerPolicies,
			LinkedList<CloneableEvictionPolicy<IN>> distributedEvictionPolicies,
			LinkedList<TriggerPolicy<IN>> centralTriggerPolicies) {

		super(userFunction);
		this.keySelector = keySelector;
		this.centralTriggerPolicies = centralTriggerPolicies;
		this.distributedTriggerPolicies = distributedTriggerPolicies;
		this.distributedEvictionPolicies = distributedEvictionPolicies;

		for (TriggerPolicy<IN> trigger : centralTriggerPolicies) {
			if (trigger instanceof ActiveTriggerPolicy) {
				this.activeCentralTriggerPolicies.add((ActiveTriggerPolicy<IN>) trigger);
			}
		}
	}

	@Override
	protected void immutableInvoke() throws Exception {
		// Prevent empty data streams
		if ((reuse = recordIterator.next(reuse)) == null) {
			throw new RuntimeException("DataStream must not be empty");
		}

		// Continuously run
		while (reuse != null) {
			WindowingInvokable<IN> groupInvokable = windowingGroups.get(keySelector.getKey(reuse
					.getObject()));
			if (groupInvokable == null) {
				groupInvokable = makeNewGroup(reuse);
			}

			// Run the precalls for central active triggers
			for (ActiveTriggerPolicy<IN> trigger : activeCentralTriggerPolicies) {
				IN[] result = trigger.preNotifyTrigger(reuse.getObject());
				for (IN in : result) {
					for (WindowingInvokable<IN> group : windowingGroups.values()) {
						group.processFakeElement(in, trigger);
					}
				}
			}

			// Process non-active central triggers
			for (TriggerPolicy<IN> triggerPolicy : centralTriggerPolicies) {
				if (triggerPolicy.notifyTrigger(reuse.getObject())) {
					currentTriggerPolicies.add(triggerPolicy);
				}
			}

			if (currentTriggerPolicies.isEmpty()) {
				// only add the element to its group
				groupInvokable.processRealElement(reuse.getObject());
			} else {
				// call user function for all groups
				for (WindowingInvokable<IN> group : windowingGroups.values()) {
					if (group == groupInvokable) {
						// process real with initialized policies
						group.processRealElement(reuse.getObject(), currentTriggerPolicies);
					} else {
						// process like a fake but also initialized with
						// policies
						group.externalTriggerFakeElement(reuse.getObject(), currentTriggerPolicies);
					}
				}
			}

			// clear current trigger list
			currentTriggerPolicies.clear();

			// Recreate the reuse-StremRecord object and load next StreamRecord
			resetReuse();
			reuse = recordIterator.next(reuse);
		}

		// Stop all remaining threads from policies
		for (Thread t : activePolicyThreads) {
			t.interrupt();
		}

		// finally trigger the buffer.
		for (WindowingInvokable<IN> group : windowingGroups.values()) {
			group.emitFinalWindow(centralTriggerPolicies);
		}

	}

	/**
	 * This method creates a new group. The method gets called in case an
	 * element arrives which has a key which was not seen before. The method
	 * created a nested {@link WindowingInvokable} and therefore created clones
	 * of all distributed trigger and eviction policies.
	 * 
	 * @param element
	 *            The element which leads to the generation of a new group
	 *            (previously unseen key)
	 * @throws Exception
	 *             In case the {@link KeySelector} throws an exception in
	 *             {@link KeySelector#getKey(Object)}, the exception is not
	 *             catched by this method.
	 */
	private WindowingInvokable<IN> makeNewGroup(StreamRecord<IN> element) throws Exception {
		// clone the policies
		LinkedList<TriggerPolicy<IN>> clonedDistributedTriggerPolicies = new LinkedList<TriggerPolicy<IN>>();
		LinkedList<EvictionPolicy<IN>> clonedDistributedEvictionPolicies = new LinkedList<EvictionPolicy<IN>>();
		for (CloneableTriggerPolicy<IN> trigger : this.distributedTriggerPolicies) {
			clonedDistributedTriggerPolicies.add(trigger.clone());
		}
		for (CloneableEvictionPolicy<IN> eviction : this.distributedEvictionPolicies) {
			clonedDistributedEvictionPolicies.add(eviction.clone());
		}

		@SuppressWarnings("unchecked")
		WindowingInvokable<IN> groupInvokable = new WindowingInvokable<IN>(
				(ReduceFunction<IN>) userFunction, clonedDistributedTriggerPolicies,
				clonedDistributedEvictionPolicies);
		groupInvokable.initialize(collector, recordIterator, inSerializer, isMutable);
		groupInvokable.open(this.parameters);
		windowingGroups.put(keySelector.getKey(element.getObject()), groupInvokable);

		return groupInvokable;
	}

	@Override
	protected void mutableInvoke() throws Exception {
		if (LOG.isInfoEnabled()) {
			LOG.info("There is currently no mutable implementation of this operator. Immutable version is used.");
		}
		immutableInvoke();
	}

	@Override
	protected void callUserFunction() throws Exception {
		// This method gets never called directly. The user function calls are
		// all delegated to the invokable instanced which handle/represent the
		// groups.
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
	 * This callback class allows to handle the the callbacks done by threads
	 * defined in active trigger policies
	 * 
	 * @see ActiveTriggerPolicy#createActiveTriggerRunnable(ActiveTriggerCallback)
	 */
	private class WindowingCallback implements ActiveTriggerCallback<IN> {
		private ActiveTriggerPolicy<IN> policy;

		public WindowingCallback(ActiveTriggerPolicy<IN> policy) {
			this.policy = policy;
		}

		@Override
		public void sendFakeElement(IN datapoint) {
			for (WindowingInvokable<IN> group : windowingGroups.values()) {
				group.processFakeElement(datapoint, policy);
			}
		}

	}
}
