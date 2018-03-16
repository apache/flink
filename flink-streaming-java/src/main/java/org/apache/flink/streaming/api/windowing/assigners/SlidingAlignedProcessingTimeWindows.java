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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * This is a special window assigner used to tell the system to use the
 * <i>"Fast Aligned Processing Time Window Operator"</i> for windowing.
 *
 * <p>Prior Flink versions used that operator automatically for simple processing time
 * windows (tumbling and sliding) when no custom trigger and no evictor was specified.
 * In the current Flink version, that operator is only used when programs explicitly
 * specify this window assigner. This is only intended for special cases where programs relied on
 * the better performance of the fast aligned window operator, and are willing to accept the lack
 * of support for various features as indicated below:
 *
 * <ul>
 *     <li>No custom state backend can be selected, the operator always stores data on the Java heap.</li>
 *     <li>The operator does not support key groups, meaning it cannot change the parallelism.</li>
 *     <li>Future versions of Flink may not be able to resume from checkpoints/savepoints taken by this
 *         operator.</li>
 * </ul>
 *
 * <p>Future implementation plans: We plan to add some of the optimizations used by this operator to
 * the general window operator, so that future versions of Flink will not have the performance/functionality
 * trade-off any more.
 *
 * <p>Note on implementation: The concrete operator instantiated by this assigner is either the
 * {@link org.apache.flink.streaming.runtime.operators.windowing.AggregatingProcessingTimeWindowOperator}
 * or {@link org.apache.flink.streaming.runtime.operators.windowing.AccumulatingProcessingTimeWindowOperator}.
 */
@PublicEvolving
public final class SlidingAlignedProcessingTimeWindows extends BaseAlignedWindowAssigner {

	private static final long serialVersionUID = 3695562702662473688L;

	private final long slide;

	public SlidingAlignedProcessingTimeWindows(long size, long slide) {
		super(size);
		this.slide = slide;
	}

	public long getSlide() {
		return slide;
	}

	/**
	 * Creates a new {@code SlidingAlignedProcessingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to sliding time windows based on the element timestamp.
	 *
	 * @param size The size of the generated windows.
	 * @param slide The slide interval of the generated windows.
	 */
	public static SlidingAlignedProcessingTimeWindows of(Time size, Time slide) {
		return new SlidingAlignedProcessingTimeWindows(size.toMilliseconds(), slide.toMilliseconds());
	}
}
