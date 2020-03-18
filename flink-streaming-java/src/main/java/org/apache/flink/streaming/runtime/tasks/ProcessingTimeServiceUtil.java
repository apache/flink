/*
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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;

/**
 * Utility for classes that implements the {@link ProcessingTimeService} interface.
 */
@Internal
public class ProcessingTimeServiceUtil {

	/**
	 * Returns the remaining delay of the processing time specified by {@code processingTimestamp}.
	 *
	 * @param processingTimestamp the processing time in milliseconds
	 * @param currentTimestamp the current processing timestamp; it usually uses
	 *        {@link ProcessingTimeService#getCurrentProcessingTime()} to get
	 * @return the remaining delay of the processing time
	 */
	public static long getProcessingTimeDelay(long processingTimestamp, long currentTimestamp) {

		// delay the firing of the timer by 1 ms to align the semantics with watermark. A watermark
		// T says we won't see elements in the future with a timestamp smaller or equal to T.
		// With processing time, we therefore need to delay firing the timer by one ms.
		return Math.max(processingTimestamp - currentTimestamp, 0) + 1;
	}
}
