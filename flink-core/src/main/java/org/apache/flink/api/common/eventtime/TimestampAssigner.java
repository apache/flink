/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;

/**
 * A {@code TimestampAssigner} assigns event time timestamps to elements.
 * These timestamps are used by all functions that operate on event time,
 * for example event time windows.
 *
 * <p>Timestamps can be an arbitrary {@code long} value, but all built-in implementations
 * represent it as the milliseconds since the Epoch (midnight, January 1, 1970 UTC),
 * the same way as {@link System#currentTimeMillis()} does it.
 *
 * @param <T> The type of the elements to which this assigner assigns timestamps.
 */
@Public
@FunctionalInterface
public interface TimestampAssigner<T> extends Function {

	/**
	 * Assigns a timestamp to an element, in milliseconds since the Epoch. This is independent of
	 * any particular time zone or calendar.
	 *
	 * <p>The method is passed the previously assigned timestamp of the element.
	 * That previous timestamp may have been assigned from a previous assigner,
	 * by ingestion time. If the element did not carry a timestamp before, this value is
	 * {@code Long.MIN_VALUE}.
	 *
	 * @param element The element that the timestamp will be assigned to.
	 * @param recordTimestamp The current internal timestamp of the element,
	 *                         or a negative value, if no timestamp has been assigned yet.
	 * @return The new timestamp.
	 */
	long extractTimestamp(T element, long recordTimestamp);
}
