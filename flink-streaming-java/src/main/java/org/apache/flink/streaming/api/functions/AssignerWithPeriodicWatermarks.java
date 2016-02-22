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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * The {@code AssignerWithPeriodicWatermarks} assigns event time timestamps to elements,
 * and generates low watermarks that signal event time progress within the stream.
 * These timestamps and watermarks are used by functions and operators that operate
 * on event time, for example event time windows.
 * 
 * <p>This class is used to generate watermarks in a periodical interval.
 * At most every {@code i} milliseconds (configured via
 * {@link ExecutionConfig#getAutoWatermarkInterval()}, the system will call the
 * {@link #getCurrentWatermark()} method to probe for the next watermark value.
 * The system will generate a new watermark, if the probed value is larger than
 * zero and larger than the previous watermark.
 * 
 * <p>The system may call the {@link #getCurrentWatermark()} method less often than every
 * {@code i} milliseconds, of no new elements arrived since the last call to the
 * method.
 *
 * <p>Timestamps and watermarks are defined as {@code longs} that represent the
 * milliseconds since the Epoch (midnight, January 1, 1970 UTC).
 * A watermark with a certain value {@code t} indicates that no elements with event
 * timestamps {@code x}, where {@code x} is lower or equal to {@code t}, will occur any more.
 * 
 * @param <T> The type of the elements to which this assigner assigns timestamps.
 * 
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
public interface AssignerWithPeriodicWatermarks<T> extends TimestampAssigner<T> {

	/**
	 * Returns the current watermark. This method is periodically called by the
	 * system to retrieve the current watermark. The method may return null to
	 * indicate that no new Watermark is available.
	 * 
	 * <p>The returned watermark will be emitted only if it is non-null and larger
	 * than the previously emitted watermark. If the current watermark is still
	 * identical to the previous one, no progress in event time has happened since
	 * the previous call to this method.
	 * 
	 * <p>If this method returns a value that is smaller than the previously returned watermark,
	 * then the implementation does not properly handle the event stream timestamps.
	 * In that case, the returned watermark will not be emitted (to preserve the contract of
	 * ascending watermarks), and the violation will be logged and registered in the metrics.
	 *     
	 * <p>The interval in which this method is called and Watermarks are generated
	 * depends on {@link ExecutionConfig#getAutoWatermarkInterval()}.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 * @see ExecutionConfig#getAutoWatermarkInterval()
	 */
	Watermark getCurrentWatermark();
}
