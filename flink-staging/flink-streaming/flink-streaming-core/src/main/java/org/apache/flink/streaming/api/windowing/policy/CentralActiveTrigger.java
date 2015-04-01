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
 * Interface for defining grouped windowing policies which can interact with
 * other groups to trigger on the latest available information globally
 * available to all groups.</p> At predefined time intervals the discretizers
 * takes the last globally seen element, and notifies all groups (but the one
 * that already have seen the object). This allows to trigger before an element
 * comes from the next window for a specific group. This pattern can be
 * used for instance in time policies to regularly broadcast the current time to
 * all groups.
 */
public interface CentralActiveTrigger<DATA> extends CloneableTriggerPolicy<DATA> {

	/**
	 * This method is called to broadcast information about the last globally
	 * seen data point to all triggers. The number of elements returned in the
	 * array will determine the number of triggers at that point, while the
	 * elements themselves are used only for active eviction.
	 * 
	 * @param datapoint
	 *            The last globally seen data
	 * @return An object of fake elements. If returned null or empty list, no
	 *         triggers will occur.
	 */
	public Object[] notifyOnLastGlobalElement(DATA datapoint);

}
