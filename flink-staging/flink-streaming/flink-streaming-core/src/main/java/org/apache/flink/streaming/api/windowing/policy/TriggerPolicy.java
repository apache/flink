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

import java.io.Serializable;

/**
 * Proves and returns if a new window should be started. In case the trigger
 * occurs (return value true) the UDF will be executed on the current element
 * buffer without the last added element which is provided as parameter. This
 * element will be added to the buffer after the execution of the UDF.
 * 
 * @param <DATA>
 *            The data type which can be handled by this policy
 */
public interface TriggerPolicy<DATA> extends Serializable {

	/**
	 * Proves and returns if a new window should be started. In case the trigger
	 * occurs (return value true) the UDF will be executed on the current
	 * element buffer without the last added element which is provided as
	 * parameter. This element will be added to the buffer after the execution
	 * of the UDF.
	 * 
	 * There are possibly different strategies for eviction and triggering: 1)
	 * including last data point: Better/faster for count eviction 2) excluding
	 * last data point: Essentially required for time based eviction and delta
	 * rules As 2) is required for some policies and the benefit of using 1) is
	 * small for the others, policies are implemented according to 2).
	 *
	 * @param datapoint
	 *            the data point which arrived
	 * @return true if the current windows should be closed, otherwise false. In
	 *         true case the given data point will be part of the next window
	 *         and will not be included in the current one.
	 */
	public boolean notifyTrigger(DATA datapoint);

}
