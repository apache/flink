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

package org.apache.flink.state.api.output.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.function.Supplier;

/**
 * A timer service that initializes its internal
 * timer service lazily.
 */
@Internal
public class LazyTimerService implements TimerService {

	private final Supplier<InternalTimerService<VoidNamespace>> supplier;

	private final ProcessingTimeService processingTimeService;

	private InternalTimerService<VoidNamespace> internalTimerService;

	LazyTimerService(Supplier<InternalTimerService<VoidNamespace>> supplier, ProcessingTimeService processingTimeService) {
		this.supplier = supplier;
		this.processingTimeService = processingTimeService;
	}

	@Override
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		// The watermark does not advance
		// when bootstrapping state.
		return Long.MIN_VALUE;
	}

	@Override
	public void registerProcessingTimeTimer(long time) {
		ensureInitialized();
		internalTimerService.registerEventTimeTimer(VoidNamespace.INSTANCE, time);
	}

	@Override
	public void registerEventTimeTimer(long time) {
		ensureInitialized();
		internalTimerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, time);
	}

	@Override
	public void deleteProcessingTimeTimer(long time) {
		ensureInitialized();
		internalTimerService.deleteProcessingTimeTimer(VoidNamespace.INSTANCE, time);
	}

	@Override
	public void deleteEventTimeTimer(long time) {
		ensureInitialized();
		internalTimerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, time);
	}

	private void ensureInitialized() {
		if (internalTimerService == null) {
			internalTimerService = supplier.get();
		}
	}
}
