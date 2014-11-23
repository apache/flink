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
 * This interface is used for active eviction policies. beside the functionality
 * inherited from {@link EvictionPolicy} it provides a method which gets called
 * to notify on fake elements.
 * 
 * In case an eviction policy implements this interface instead of the
 * {@link EvictionPolicy} interface, not only the real but also fake data points
 * will cause a notification of the eviction.
 * 
 * Fake data points are mostly used in windowing based on time to trigger and
 * evict even if no element arrives at all during a windows duration.
 */
public interface ActiveEvictionPolicy<DATA> extends EvictionPolicy<DATA> {

	/**
	 * Proves if and how many elements should be deleted from the element
	 * buffer. The eviction takes place after the trigger and after the call to
	 * the UDF. This method is only called with fake elements.
	 * 
	 * Note: Fake elements are always considered as triggered. Therefore this
	 * method does not have a triggered parameter.
	 * 
	 * @param datapoint
	 *            the current fake data point
	 * @param bufferSize
	 *            the current size of the buffer (only real elements are
	 *            counted)
	 * @return the number of elements to delete from the buffer (only real
	 *         elements are counted)
	 */
	public int notifyEvictionWithFakeElement(Object datapoint, int bufferSize);

}
