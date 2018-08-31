/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util.clock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link Clock} implementation which allows to advance time manually.
 */
public class ManualClock extends Clock {

	private AtomicLong currentTime = new AtomicLong(0L);

	@Override
	public long absoluteTimeMillis() {
		return currentTime.get() / 1_000L;
	}

	@Override
	public long relativeTimeMillis() {
		return currentTime.get() / 1_000L;
	}

	@Override
	public long relativeTimeNanos() {
		return currentTime.get();
	}

	public void advanceTime(long duration, TimeUnit timeUnit) {
		currentTime.addAndGet(timeUnit.toNanos(duration));
	}
}
