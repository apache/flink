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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.WindowEvent;
import org.apache.flink.streaming.api.windowing.policy.ActiveEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerCallback;
import org.apache.flink.streaming.api.windowing.policy.ActiveTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * This operator represents the discretization step of a window transformation.
 * The user supplied eviction and trigger policies are applied to create the
 * {@link StreamWindow} that will be further transformed in the next stages.
 */
public class StreamDiscretizer<IN>
		extends AbstractStreamOperator<WindowEvent<IN>>
		implements OneInputStreamOperator<IN, WindowEvent<IN>> {

	private static final long serialVersionUID = 1L;

	protected TriggerPolicy<IN> triggerPolicy;
	protected EvictionPolicy<IN> evictionPolicy;
	private boolean isActiveTrigger;
	private boolean isActiveEviction;
	private int bufferSize = 0;

	private transient Thread activePolicyThread;

	protected WindowEvent<IN> windowEvent = new WindowEvent<IN>();

	public StreamDiscretizer(TriggerPolicy<IN> triggerPolicy, EvictionPolicy<IN> evictionPolicy) {
		this.triggerPolicy = triggerPolicy;
		this.evictionPolicy = evictionPolicy;

		this.isActiveTrigger = triggerPolicy instanceof ActiveTriggerPolicy;
		this.isActiveEviction = evictionPolicy instanceof ActiveEvictionPolicy;

		this.chainingStrategy = ChainingStrategy.FORCE_ALWAYS;
	}

	public TriggerPolicy<IN> getTrigger() {
		return triggerPolicy;
	}

	public EvictionPolicy<IN> getEviction() {
		return evictionPolicy;
	}

	@Override
	public void processElement(IN element) throws Exception {
		processRealElement(element);
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

		// Setting the input element in order to avoid NullFieldException when triggering on fake element
		windowEvent.setElement(input);
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

		output.collect(windowEvent.setElement(input));
		bufferSize++;

	}

	/**
	 * This method triggers on an arrived fake element The method is
	 * synchronized to ensure that it cannot interleave with
	 * {@link StreamDiscretizer#processRealElement(Object)}
	 * 
	 * @param input
	 *            a fake input element
	 */
	@SuppressWarnings("unchecked")
	protected synchronized void triggerOnFakeElement(Object input) {
		if (isActiveEviction) {
			activeEvict(input);
			emitWindow();
		} else {
			emitWindow();
			evict((IN) input, true);
		}
	}

	/**
	 * This method emits the content of the buffer as a new {@link StreamWindow}
	 * if not empty
	 */
	protected void emitWindow() {
		output.collect(windowEvent.setTrigger());
	}

	private void activeEvict(Object input) {
		int numToEvict = 0;

		if (isActiveEviction) {
			ActiveEvictionPolicy<IN> ep = (ActiveEvictionPolicy<IN>) evictionPolicy;
			numToEvict = ep.notifyEvictionWithFakeElement(input, bufferSize);
		}

		if (numToEvict > 0) {
			output.collect(windowEvent.setEviction(numToEvict));
			bufferSize -= numToEvict;
			bufferSize = bufferSize >= 0 ? bufferSize : 0;
		}
	}

	private void evict(IN input, boolean isTriggered) {
		int numToEvict = evictionPolicy.notifyEviction(input, isTriggered, bufferSize);

		if (numToEvict > 0) {
			output.collect(windowEvent.setEviction(numToEvict));
			bufferSize -= numToEvict;
			bufferSize = bufferSize >= 0 ? bufferSize : 0;
		}
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
		super.open(parameters);

		if (isActiveTrigger) {
			ActiveTriggerPolicy<IN> tp = (ActiveTriggerPolicy<IN>) triggerPolicy;

			Runnable runnable = tp.createActiveTriggerRunnable(new WindowingCallback());
			if (runnable != null) {
				activePolicyThread = new Thread(runnable);
				activePolicyThread.start();
			}
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (activePolicyThread != null) {
			activePolicyThread.interrupt();
		}

		emitWindow();
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

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof StreamDiscretizer)
				|| (other instanceof GroupedStreamDiscretizer)) {
			return false;
		} else {
			try {
				@SuppressWarnings("unchecked")
				StreamDiscretizer<IN> otherDiscretizer = (StreamDiscretizer<IN>) other;

				return triggerPolicy.equals(otherDiscretizer.triggerPolicy)
						&& evictionPolicy.equals(otherDiscretizer.evictionPolicy);

			} catch (ClassCastException e) {
				return false;
			}
		}
	}

	@Override
	public String toString() {
		return "Discretizer(Trigger: " + triggerPolicy.toString() + ", Eviction: "
				+ evictionPolicy.toString() + ")";
	}
}
