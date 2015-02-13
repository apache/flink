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

import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;

/**
 * This trigger policy triggers with regard to the time. The is measured using a
 * given {@link Timestamp} implementation. A point in time is always represented
 * as long. Therefore, parameters such as granularity and delay can be set as
 * long value as well.
 * 
 * @param <DATA>
 *            The type of the incoming data points which are processed by this
 *            policy.
 */
public class TimeTriggerPolicy<DATA> implements ActiveTriggerPolicy<DATA>,
		CloneableTriggerPolicy<DATA> {

	/**
	 * auto generated version id
	 */
	private static final long serialVersionUID = -5122753802440196719L;

	protected long startTime;
	public long granularity;
	public TimestampWrapper<DATA> timestampWrapper;
	protected long delay;

	/**
	 * This trigger policy triggers with regard to the time. The is measured
	 * using a given {@link Timestamp} implementation. A point in time is always
	 * represented as long. Therefore, parameters such as granularity can be set
	 * as long value as well. If this value for the granularity is set to 2 for
	 * example, the policy will trigger at every second point in time.
	 * 
	 * @param granularity
	 *            The granularity of the trigger. If this value is set to x the
	 *            policy will trigger at every x-th time point
	 * @param timestampWrapper
	 *            The {@link TimestampWrapper} to measure the time with. This
	 *            can be either user defined of provided by the API.
	 * @param timeWrapper
	 *            This policy creates fake elements to not miss windows in case
	 *            no element arrived within the duration of the window. This
	 *            extractor should wrap a long into such an element of type
	 *            DATA.
	 */
	public TimeTriggerPolicy(long granularity, TimestampWrapper<DATA> timestampWrapper) {
		this(granularity, timestampWrapper, 0);
	}

	/**
	 * This is mostly the same as
	 * {@link TimeTriggerPolicy#TimeTriggerPolicy(long, Timestamp)}. In addition
	 * to granularity and timestamp a delay can be specified for the first
	 * trigger. If the start time given by the timestamp is x, the delay is y,
	 * and the granularity is z, the first trigger will happen at x+y+z.
	 * 
	 * @param granularity
	 *            The granularity of the trigger. If this value is set to 2 the
	 *            policy will trigger at every second time point
	 * @param timestampWrapper
	 *            The {@link TimestampWrapper} to measure the time with. This
	 *            can be either user defined of provided by the API.
	 * @param delay
	 *            A delay for the first trigger. If the start time given by the
	 *            timestamp is x, the delay is y, and the granularity is z, the
	 *            first trigger will happen at x+y+z.
	 * @param timeWrapper
	 *            This policy creates fake elements to not miss windows in case
	 *            no element arrived within the duration of the window. This
	 *            extractor should wrap a long into such an element of type
	 *            DATA.
	 */
	public TimeTriggerPolicy(long granularity, TimestampWrapper<DATA> timestampWrapper, long delay) {
		this.startTime = timestampWrapper.getStartTime() + delay;
		this.timestampWrapper = timestampWrapper;
		this.granularity = granularity;
		this.delay = delay;
	}

	/**
	 * This method checks if we missed a window end. If this is the case we
	 * trigger the missed windows using fake elements.
	 */
	@Override
	public synchronized Object[] preNotifyTrigger(DATA datapoint) {
		LinkedList<Object> fakeElements = new LinkedList<Object>();
		// check if there is more then one window border missed
		// use > here. In case >= would fit, the regular call will do the job.
		while (timestampWrapper.getTimestamp(datapoint) >= startTime + granularity) {
			startTime += granularity;
			fakeElements.add(startTime - 1);
		}
		return (Object[]) fakeElements.toArray();
	}

	/**
	 * In case {@link DefaultTimeStamp} is used, a runnable is returned which
	 * triggers based on the current system time. If any other time measure is
	 * used the method return null.
	 * 
	 * @param callback
	 *            The object which is takes the callbacks for adding fake
	 *            elements out of the runnable.
	 * @return A runnable is returned which triggers based on the current system
	 *         time. If any other time measure is used the method return null.
	 */
	@Override
	public Runnable createActiveTriggerRunnable(ActiveTriggerCallback callback) {
		if (this.timestampWrapper.isDefaultTimestamp()) {
			return new TimeCheck(callback);
		} else {
			return null;
		}
	}

	/**
	 * This method is only called in case the runnable triggers a window end
	 * according to the {@link DefaultTimeStamp}.
	 * 
	 * @param callback
	 *            The callback object.
	 */
	public synchronized Object activeFakeElementEmission(ActiveTriggerCallback callback) {

		// start time is excluded, but end time is included: >=
		if (System.currentTimeMillis() >= startTime + granularity) {
			startTime += granularity;
			if (callback != null) {
				callback.sendFakeElement(startTime - 1);
			}
			return startTime - 1;
		}
		return null;

	}

	private class TimeCheck implements Runnable {
		ActiveTriggerCallback callback;

		public TimeCheck(ActiveTriggerCallback callback) {
			this.callback = callback;
		}

		@Override
		public void run() {
			while (true) {
				// wait for the specified granularity
				try {
					Thread.sleep(granularity);
				} catch (InterruptedException e) {
					// ignore it...
				}
				// Trigger using the respective methods. Methods are
				// synchronized to prevent race conditions between real and fake
				// elements at the policy.
				activeFakeElementEmission(callback);
			}
		}
	}

	@Override
	public synchronized boolean notifyTrigger(DATA datapoint) {
		long recordTime = timestampWrapper.getTimestamp(datapoint);
		if (recordTime >= startTime + granularity) {
			if (granularity != 0) {
				startTime = recordTime - ((recordTime - startTime) % granularity);
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public TimeTriggerPolicy<DATA> clone() {
		return new TimeTriggerPolicy<DATA>(granularity, timestampWrapper, delay);
	}

}
