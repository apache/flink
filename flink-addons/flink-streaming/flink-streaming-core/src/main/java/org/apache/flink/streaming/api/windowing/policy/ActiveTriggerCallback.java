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

/**
 * In case an {@link ActiveTriggerPolicy} is used, it can implement own
 * {@link Runnable} classes. Such {@link Runnable} classes will be executed as
 * an own thread and can submit fake elements, to the element buffer at any
 * time.
 * 
 * The factory method for runnables of the {@link ActiveTriggerPolicy} gets an
 * instance of this interface as parameter. The describes adding of elements can
 * be done by the runnable using the methods provided in this interface.
 * 
 */
public interface ActiveTriggerCallback {

	/**
	 * Submits a new fake data point to the element buffer. Such a fake element
	 * might be used to trigger at any time, but will never be included in the
	 * result of the reduce function. The submission of a fake element causes
	 * notifications only at the {@link ActiveTriggerPolicy} and
	 * {@link ActiveEvictionPolicy} implementations.
	 * 
	 * @param datapoint
	 *            the fake data point to be added
	 */
	public void sendFakeElement(Object datapoint);

}
