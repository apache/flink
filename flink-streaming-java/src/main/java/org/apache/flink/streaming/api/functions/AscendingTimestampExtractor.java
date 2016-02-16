/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.functions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface for user functions that extract timestamps from elements. The extracting timestamps
 * must be monotonically increasing.
 *
 * @param <T> The type of the elements that this function can extract timestamps from
 */
@PublicEvolving
public abstract class AscendingTimestampExtractor<T> implements TimestampExtractor<T> {

	long currentTimestamp = 0;

	/**
	 * Extracts a timestamp from an element. The timestamp must be monotonically increasing.
	 *
	 * @param element The element that the timestamp is extracted from.
	 * @param currentTimestamp The current internal timestamp of the element.
	 * @return The new timestamp.
	 */
	public abstract long extractAscendingTimestamp(T element, long currentTimestamp);

	@Override
	public final long extractTimestamp(T element, long currentTimestamp) {
		long newTimestamp = extractAscendingTimestamp(element, currentTimestamp);
		if (newTimestamp < this.currentTimestamp) {
			throw new RuntimeException("Timestamp is lower than previously extracted timestamp. " +
					"You should implement a custom TimestampExtractor.");
		}
		this.currentTimestamp = newTimestamp;
		return this.currentTimestamp;
	}

	@Override
	public final long extractWatermark(T element, long currentTimestamp) {
		return Long.MIN_VALUE;
	}

	@Override
	public final long getCurrentWatermark() {
		return currentTimestamp - 1;
	}
}
