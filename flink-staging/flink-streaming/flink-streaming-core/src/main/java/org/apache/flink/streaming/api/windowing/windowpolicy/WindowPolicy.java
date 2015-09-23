/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.windowpolicy;

import org.apache.flink.streaming.api.TimeCharacteristic;

/**
 * The base class of all window policies. Window policies define how windows
 * are formed over the data stream.
 */
public abstract class WindowPolicy implements java.io.Serializable {

	private static final long serialVersionUID = -8696529489282723113L;
	
	/**
	 * If this window policies concrete instantiation depends on the time characteristic of the
	 * dataflow (processing time, event time), then this method must be overridden to convert this
	 * policy to the respective specific instantiation.
	 * <p>
	 * The {@link Time} policy for example, will convert itself to an {@link ProcessingTime} policy,
	 * if the time characteristic is set to {@link TimeCharacteristic#ProcessingTime}.
	 * <p>
	 * By default, this method does nothing and simply returns this object itself.
	 * 
	 * @param characteristic The time characteristic of the dataflow.
	 * @return The specific instantiation of this policy, or the policy itself. 
	 */
	public WindowPolicy makeSpecificBasedOnTimeCharacteristic(TimeCharacteristic characteristic) {
		return this;
	}
	
	
	public String toString(WindowPolicy slidePolicy) {
		if (slidePolicy != null) {
			return "Window [" + toString() + ", slide=" + slidePolicy + ']';
		}
		else {
			return "Window [" + toString() + ']';
		}
	}
}
