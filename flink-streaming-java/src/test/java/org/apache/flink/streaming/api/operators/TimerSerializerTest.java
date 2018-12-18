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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Test for {@link TimerSerializer}.
 */
public class TimerSerializerTest extends SerializerTestBase<TimerHeapInternalTimer<Long, TimeWindow>> {

	private static final TypeSerializer<Long> KEY_SERIALIZER = LongSerializer.INSTANCE;
	private static final TypeSerializer<TimeWindow> NAMESPACE_SERIALIZER = new TimeWindow.Serializer();

	@Override
	protected TypeSerializer<TimerHeapInternalTimer<Long, TimeWindow>> createSerializer() {
		return new TimerSerializer<>(KEY_SERIALIZER, NAMESPACE_SERIALIZER);
	}

	@Override
	protected int getLength() {
		return Long.BYTES + KEY_SERIALIZER.getLength() + NAMESPACE_SERIALIZER.getLength();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Class<TimerHeapInternalTimer<Long, TimeWindow>> getTypeClass() {
		return (Class<TimerHeapInternalTimer<Long, TimeWindow>>) (Class<?>) TimerHeapInternalTimer.class;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TimerHeapInternalTimer<Long, TimeWindow>[] getTestData() {
		return (TimerHeapInternalTimer<Long, TimeWindow>[]) new TimerHeapInternalTimer[]{
			new TimerHeapInternalTimer<>(42L, 4711L, new TimeWindow(1000L, 2000L)),
			new TimerHeapInternalTimer<>(0L, 0L, new TimeWindow(0L, 0L)),
			new TimerHeapInternalTimer<>(1L, -1L, new TimeWindow(1L, -1L)),
			new TimerHeapInternalTimer<>(-1L, 1L, new TimeWindow(-1L, 1L)),
			new TimerHeapInternalTimer<>(Long.MAX_VALUE, Long.MIN_VALUE, new TimeWindow(Long.MAX_VALUE, Long.MIN_VALUE)),
			new TimerHeapInternalTimer<>(Long.MIN_VALUE, Long.MAX_VALUE, new TimeWindow(Long.MIN_VALUE, Long.MAX_VALUE))
		};
	}
}
