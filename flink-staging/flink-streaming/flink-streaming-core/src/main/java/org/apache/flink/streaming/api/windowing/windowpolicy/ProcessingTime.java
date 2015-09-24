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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.util.concurrent.TimeUnit;

/**
 * The definition of a processing time interval for windowing. See
 * {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime} for a definition
 * of processing time.
 */
public final class ProcessingTime extends AbstractTimePolicy {

	private static final long serialVersionUID = 7546166721132583007L;

	/** Instantiation only via factory method */
	private ProcessingTime(long size, TimeUnit unit) {
		super(size, unit);
	}

	@Override
	public ProcessingTime makeSpecificBasedOnTimeCharacteristic(TimeCharacteristic characteristic) {
		if (characteristic == TimeCharacteristic.ProcessingTime) {
			return this;
		}
		else {
			throw new InvalidProgramException(
					"Cannot use ProcessingTime policy in a dataflow that runs on " + characteristic);
		}
	}

	// ------------------------------------------------------------------------
	//  Factory
	// ------------------------------------------------------------------------

	/**
	 * Creates a processing time policy describing a processing time interval.
	 *
	 * @param size The size of the generated windows.
	 * @param unit The init (seconds, milliseconds) of the time interval.
	 * @return The processing time policy.
	 */
	public static ProcessingTime of(long size, TimeUnit unit) {
		return new ProcessingTime(size, unit);
	}
}
