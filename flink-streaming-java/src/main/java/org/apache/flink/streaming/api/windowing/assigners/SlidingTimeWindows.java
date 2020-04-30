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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * A {@link WindowAssigner} that windows elements into sliding windows based on the timestamp of the
 * elements. Windows can possibly overlap.
 *
 * @deprecated Please use {@link SlidingEventTimeWindows}.
 */
@PublicEvolving
@Deprecated
public class SlidingTimeWindows extends SlidingEventTimeWindows {
	private static final long serialVersionUID = 1L;

	private SlidingTimeWindows(long size, long slide) {
		super(size, slide, 0);
	}

	/**
	 * Creates a new {@code SlidingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to sliding time windows based on the element timestamp.
	 *
	 * @deprecated Please use {@link SlidingEventTimeWindows#of(Time, Time)}.
	 *
	 * @param size The size of the generated windows.
	 * @param slide The slide interval of the generated windows.
	 * @return The time policy.
	 */
	@Deprecated()
	public static SlidingTimeWindows of(Time size, Time slide) {
		return new SlidingTimeWindows(size.toMilliseconds(), slide.toMilliseconds());
	}
}
