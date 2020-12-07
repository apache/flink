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

import javax.annotation.Nullable;

/**
 * The {@code AssignerWithPeriodicWatermarks} assigns event time timestamps to elements,
 * and generates low watermarks that signal event time progress within the stream.
 * These timestamps and watermarks are used by functions and operators that operate
 * on event time, for example event time windows.
 *
 * <p>Use this class to generate watermarks in a periodical interval.
 * At most every {@code i} milliseconds (configured via
 * {@link ExecutionConfig#getAutoWatermarkInterval()}), the system will call the
 * {@link #getCurrentWatermark()} method to probe for the next watermark value.
 * The system will generate a new watermark, if the probed value is non-null
 * and has a timestamp larger than that of the previous watermark (to preserve
 * the contract of ascending watermarks).
 *
 * <p>The system may call the {@link #getCurrentWatermark()} method less often than every
 * {@code i} milliseconds, if no new elements arrived since the last call to the
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
@Deprecated
public interface AssignerWithPeriodicWatermarks<T> extends TimestampAssigner<T> {

	/**
	 * Returns the current watermark. This method is periodically called by the
	 * system to retrieve the current watermark. The method may return {@code null} to
	 * indicate that no new Watermark is available.
	 *
	 * <p>The returned watermark will be emitted only if it is non-null and its timestamp
	 * is larger than that of the previously emitted watermark (to preserve the contract of
	 * ascending watermarks). If the current watermark is still
	 * identical to the previous one, no progress in event time has happened since
	 * the previous call to this method. If a null value is returned, or the timestamp
	 * of the returned watermark is smaller than that of the last emitted one, then no
	 * new watermark will be generated.
	 *
	 * <p>The interval in which this method is called and Watermarks are generated
	 * depends on {@link ExecutionConfig#getAutoWatermarkInterval()}.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 * @see ExecutionConfig#getAutoWatermarkInterval()
	 *
	 * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
	 */
	@Nullable
	Watermark getCurrentWatermark();
}
