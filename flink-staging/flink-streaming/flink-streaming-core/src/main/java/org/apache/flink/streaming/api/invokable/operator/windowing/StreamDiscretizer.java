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

import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerCallback;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

public class StreamDiscretizer<IN> extends StreamInvokable<IN, StreamWindow<IN>> {

	/**
	 * Auto-generated serial version UID
	 */
	private static final long serialVersionUID = -8038984294071650730L;

	private TriggerPolicy<IN> triggerPolicy;
	private EvictionPolicy<IN> evictionPolicy;
	private boolean isActiveTrigger;
	private boolean isActiveEviction;
	private Thread activePolicyThread;
	protected LinkedList<IN> buffer;

	public StreamDiscretizer(TriggerPolicy<IN> triggerPolicy, EvictionPolicy<IN> evictionPolicy) {
		super(null);

		this.triggerPolicy = triggerPolicy;
		this.evictionPolicy = evictionPolicy;

		this.isActiveTrigger = triggerPolicy instanceof ActiveTriggerPolicy;
		this.isActiveEviction = evictionPolicy instanceof ActiveEvictionPolicy;

		this.buffer = new LinkedList<IN>();
	}

	@Override
	public void invoke() throws Exception {

		// Continuously run
		while (readNext() != null) {
			processRealElement(nextObject);
		}

		if (activePolicyThread != null) {
			activePolicyThread.interrupt();
		}

		emitFinalWindow();

	}

	/**
	 * This method processed an arrived real element The method is synchronized
	 * to ensure that it cannot interleave with
	 * {@link StreamDiscretizer#triggerOnFakeElement(Object)}
	 * 
	 * @param input
	 *            a real input element
	 */
	protected synchronized void processRealElement(IN input) {

		if (isActiveTrigger) {
			ActiveTriggerPolicy<IN> trigger = (ActiveTriggerPolicy<IN>) triggerPolicy;
			Object[] result = trigger.preNotifyTrigger(input);
			for (Object in : result) {
				triggerOnFakeElement(in);
			}
		}

		boolean isTriggered = false;

		if (triggerPolicy.notifyTrigger(input)) {
			emitWindow();
			isTriggered = true;
		}

		evict(input, isTriggered);

		buffer.add(input);

	}

	/**
	 * This method triggers on an arrived fake element The method is
	 * synchronized to ensure that it cannot interleave with
	 * {@link StreamDiscretizer#processRealElement(Object)}
	 * 
	 * @param input
	 *            a fake input element
	 */
	protected synchronized void triggerOnFakeElement(Object input) {
		activeEvict(input);
		emitWindow();
	}

	/**
	 * This method emits the content of the buffer as a new {@link StreamWindow}
	 */
	protected void emitWindow() {
		StreamWindow<IN> currentWindow = new StreamWindow<IN>();
		currentWindow.addAll(buffer);
		collector.collect(currentWindow);
	}

	private void activeEvict(Object input) {
		int numToEvict = 0;

		if (isActiveEviction) {
			ActiveEvictionPolicy<IN> ep = (ActiveEvictionPolicy<IN>) evictionPolicy;
			numToEvict = ep.notifyEvictionWithFakeElement(input, buffer.size());
		}

		evictFromBuffer(numToEvict);
	}

	private void evict(IN input, boolean isTriggered) {
		int numToEvict = evictionPolicy.notifyEviction(input, isTriggered, buffer.size());

		evictFromBuffer(numToEvict);
	}

	private void evictFromBuffer(int n) {
		for (int i = 0; i < n; i++) {
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
	 * This function emits the partial windows at the end of the stream
	 */
	protected void emitFinalWindow() {
		if (!buffer.isEmpty()) {
			emitWindow();
		}
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
		super.open(parameters);

		if (isActiveTrigger) {
			ActiveTriggerPolicy<IN> tp = (ActiveTriggerPolicy<IN>) triggerPolicy;

			Runnable runnable = tp.createActiveTriggerRunnable(new WindowingCallback());
			if (activePolicyThread != null) {
				activePolicyThread = new Thread(runnable);
				activePolicyThread.start();
			}
		}
	}

	@Override
	public String toString() {
		return buffer.toString();
	}

	/**
	 * This class allows the active trigger thread to call back and push fake
	 * elements at any time.
	 */
	private class WindowingCallback implements ActiveTriggerCallback {

		@Override
		public void sendFakeElement(Object datapoint) {
			triggerOnFakeElement(datapoint);
		}

	}

}
