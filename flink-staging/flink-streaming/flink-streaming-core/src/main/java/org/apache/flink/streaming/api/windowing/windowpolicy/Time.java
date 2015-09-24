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

import java.util.concurrent.TimeUnit;

/**
 * The definition of a time interval for windowing. The time characteristic referred
 * to is the default time characteristic set on the execution environment.
 */
public final class Time extends AbstractTimePolicy {

	private static final long serialVersionUID = 3197290738634320211L;

	/** Instantiation only via factory method */
	private Time(long size, TimeUnit unit) {
		super(size, unit);
	}

	@Override
	public AbstractTimePolicy makeSpecificBasedOnTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		switch (timeCharacteristic) {
			case ProcessingTime:
				return ProcessingTime.of(getSize(), getUnit());
			case IngestionTime:
			case EventTime:
				return EventTime.of(getSize(), getUnit());
			default:
				throw new IllegalArgumentException("Unknown time characteristic");
		}
	}

	// ------------------------------------------------------------------------
	//  Factory
	// ------------------------------------------------------------------------

	/**
	 * Creates a time policy describing a processing time interval. The policy refers to the
	 * time characteristic that is set on the dataflow via
	 * {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#
	 * setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic)}.
	 *
	 * @param size The size of the generated windows.
	 * @param unit The init (seconds, milliseconds) of the time interval.
	 * @return The time policy.
	 */
	public static Time of(long size, TimeUnit unit) {
		return new Time(size, unit);
	}
}
