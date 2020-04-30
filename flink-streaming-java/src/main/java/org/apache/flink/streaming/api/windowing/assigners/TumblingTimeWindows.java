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
 * A {@link WindowAssigner} that windows elements into windows based on the timestamp of the
 * elements. Windows cannot overlap.
 *
 * @deprecated Please use {@link TumblingEventTimeWindows}.
 */
@PublicEvolving
@Deprecated
public class TumblingTimeWindows extends TumblingEventTimeWindows {
	private static final long serialVersionUID = 1L;

	private TumblingTimeWindows(long size) {
		super(size, 0);
	}

	/**
	 * Creates a new {@code TumblingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp.
	 *
	 * @deprecated Please use {@link TumblingEventTimeWindows#of(Time)}.
	 *
	 * @param size The size of the generated windows.
	 * @return The time policy.
	 */
	@Deprecated()
	public static TumblingTimeWindows of(Time size) {
		return new TumblingTimeWindows(size.toMilliseconds());
	}
}
