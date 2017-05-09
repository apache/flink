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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * Interface for user functions that extract timestamps from elements.
 *
 * <p>The extractor must also keep track of the current watermark. The system will periodically
 * retrieve this watermark using {@link #getCurrentWatermark()} and submit it throughout the
 * topology.
 *
 * <p>Note: If you know that timestamps are monotonically increasing you can use
 * {@link AscendingTimestampExtractor}. This will keep track of watermarks.
 *
 * @param <T> The type of the elements that this function can extract timestamps from
 *
 * @deprecated This class has been replaced by {@link AssignerWithPeriodicWatermarks} and
 *             {@link AssignerWithPunctuatedWatermarks}
 *
 * @see AssignerWithPeriodicWatermarks
 * @see AssignerWithPunctuatedWatermarks
 */
@Deprecated
public interface TimestampExtractor<T> extends Function {

	/**
	 * Extracts a timestamp from an element.
	 *
	 * @param element The element that the timestamp is extracted from.
	 * @param currentTimestamp The current internal timestamp of the element.
	 * @return The new timestamp.
	 */
	long extractTimestamp(T element, long currentTimestamp);

	/**
	 * Asks the extractor if it wants to emit a watermark now that it has seen the given element.
	 * This is called right after {@link #extractTimestamp}. With the same element. The method
	 * can return {@code Long.MIN_VALUE} to indicate that no watermark should be emitted, a value of 0 or
	 * greater will be emitted as a watermark if it is higher than the last-emitted watermark.
	 *
	 * @param element The element that we last saw.
	 * @param currentTimestamp The current timestamp of the element that we last saw.
	 * @return {@code Long.MIN_VALUE} if no watermark should be emitted, positive value for
	 *          emitting this value as a watermark.
	 */
	long extractWatermark(T element, long currentTimestamp);

	/**
	 * Returns the current watermark. This is periodically called by the system to determine
	 * the current watermark and forward it.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	long getCurrentWatermark();
}
