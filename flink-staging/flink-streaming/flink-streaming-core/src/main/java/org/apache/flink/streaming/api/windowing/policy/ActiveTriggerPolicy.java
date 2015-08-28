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

import org.apache.flink.streaming.api.windowing.helper.Timestamp;

/**
 * This interface extends the {@link TriggerPolicy} interface with functionality
 * for active triggers. Active triggers can act in two ways:
 * 
 * 1) Whenever an element arrives at the operator, the
 * {@link ActiveTriggerPolicy#preNotifyTrigger(Object)} method gets called
 * first. It can return zero ore more fake data points which will be added
 * before the currently arrived real element gets processed. This allows to
 * handle empty windows in time based windowing with an user defined
 * {@link Timestamp}. Triggers are not called on fake datapoint. A fake
 * datapoint is always considered as triggered.
 * 
 * 2) An active trigger has a factory method for a runnable. This factory method
 * gets called at the start up of the operator. The returned runnable will be
 * executed in its own thread and can submit fake elements at any time through an
 * {@link ActiveTriggerCallback}. This allows to have time based triggers based
 * on any system internal time measure. Triggers are not called on fake
 * datapoint. A fake datapoints is always considered as triggered.
 * 
 * @param <DATA>
 *            The data type which can be handled by this policy
 */
public interface ActiveTriggerPolicy<DATA> extends TriggerPolicy<DATA> {

	/**
	 * Whenever an element arrives at the operator, the
	 * {@link ActiveTriggerPolicy#preNotifyTrigger(Object)} method gets called
	 * first. It can return zero ore more fake data points which will be added
	 * before the the currently arrived real element gets processed. This allows
	 * to handle empty windows in time based windowing with an user defined
	 * {@link Timestamp}. Triggers are not called on fake datapoints. A fake
	 * datapoint is always considered as triggered.
	 * 
	 * @param datapoint
	 *            the data point which arrived at the operator
	 * @return zero ore more fake data points which will be added before the the
	 *         currently arrived real element gets processed.
	 */
	public Object[] preNotifyTrigger(DATA datapoint);

	/**
	 * This is the factory method for a runnable. This factory method gets
	 * called at the start up of the operator. The returned runnable will be
	 * executed in its own thread and can submit fake elements at any time through
	 * an {@link ActiveTriggerCallback}. This allows to have time based triggers
	 * based on any system internal time measure. Triggers are not called on
	 * fake datapoints. A fake datapoint is always considered as triggered.
	 * 
	 * @param callback
	 *            A callback object which allows to add fake elements from
	 *            within the returned {@link Runnable}.
	 * @return The runnable implementation or null in case there is no. In case
	 *         an {@link ActiveTriggerPolicy} is used, it can implement own
	 *         {@link Runnable} classes. Such {@link Runnable} classes will be
	 *         executed as an own thread and can submit fake elements, to the
	 *         element buffer at any time.
	 */
	public Runnable createActiveTriggerRunnable(ActiveTriggerCallback callback);

}
