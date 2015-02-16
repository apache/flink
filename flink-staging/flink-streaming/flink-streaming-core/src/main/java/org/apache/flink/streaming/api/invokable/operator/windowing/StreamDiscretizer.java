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

import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerCallback;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;

/**
 * This invokable represents the discretization step of a window transformation.
 * The user supplied eviction and trigger policies are applied to create the
 * {@link StreamWindow} that will be further transformed in the next stages.
 * </p> To allow pre-aggregations supply an appropriate {@link WindowBuffer}
 */
public class StreamDiscretizer<IN> extends StreamInvokable<IN, StreamWindow<IN>> {

	/**
	 * Auto-generated serial version UID
	 */
	private static final long serialVersionUID = -8038984294071650730L;

	protected TriggerPolicy<IN> triggerPolicy;
	protected EvictionPolicy<IN> evictionPolicy;
	private boolean isActiveTrigger;
	private boolean isActiveEviction;
	private Thread activePolicyThread;
	public int emptyCount = 0;

	protected WindowBuffer<IN> bufferHandler;

	public StreamDiscretizer(TriggerPolicy<IN> triggerPolicy, EvictionPolicy<IN> evictionPolicy,
			WindowBuffer<IN> bufferHandler) {
		super(null);

		this.triggerPolicy = triggerPolicy;
		this.evictionPolicy = evictionPolicy;

		this.isActiveTrigger = triggerPolicy instanceof ActiveTriggerPolicy;
		this.isActiveEviction = evictionPolicy instanceof ActiveEvictionPolicy;

		this.bufferHandler = bufferHandler;
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

		emitWindow();

	}

	/**
	 * This method processed an arrived real element The method is synchronized
	 * to ensure that it cannot interleave with
	 * {@link StreamDiscretizer#triggerOnFakeElement(Object)}
	 * 
	 * @param input
	 *            a real input element
	 * @throws Exception
	 */
	protected synchronized void processRealElement(IN input) throws Exception {

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

		bufferHandler.store(input);

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

	protected synchronized void externalTriggerOnFakeElement(Object input) {
		emitWindow();
		activeEvict(input);
	}

	/**
	 * This method emits the content of the buffer as a new {@link StreamWindow}
	 * if not empty
	 */
	protected void emitWindow() {
		if (bufferHandler.emitWindow(collector)) {
			emptyCount = 0;
		} else {
			emptyCount++;
		}
	}

	private void activeEvict(Object input) {
		int numToEvict = 0;

		if (isActiveEviction) {
			ActiveEvictionPolicy<IN> ep = (ActiveEvictionPolicy<IN>) evictionPolicy;
			numToEvict = ep.notifyEvictionWithFakeElement(input, bufferHandler.size());
		}

		bufferHandler.evict(numToEvict);
	}

	private void evict(IN input, boolean isTriggered) {
		int numToEvict = evictionPolicy.notifyEviction(input, isTriggered, bufferHandler.size());

		bufferHandler.evict(numToEvict);
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
		return bufferHandler.toString();
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
