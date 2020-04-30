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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.table.dataformat.BaseRow;

import java.io.IOException;
import java.util.Optional;

/**
 * An abstract {@link TwoInputStreamOperator} that allows its subclasses to clean
 * up their state based on a TTL. This TTL should be specified in the provided
 * {@code minRetentionTime} and {@code maxRetentionTime}.
 *
 * <p>For each known key, this operator registers a timer (in processing time) to
 * fire after the TTL expires. When the timer fires, the subclass can decide which
 * state to cleanup and what further action to take.
 *
 * <p>This class takes care of maintaining at most one timer per key.
 *
 * <p><b>IMPORTANT NOTE TO USERS:</b> When extending this class, do not use processing time
 * timers in your business logic. The reason is that:
 *
 * <p>1) if your timers collide with clean up timers and you delete them, then state
 * clean-up will not be performed, and
 *
 * <p>2) (this one is the reason why this class does not allow to override the onProcessingTime())
 * the onProcessingTime with your logic would be also executed on each clean up timer.
 */
@Internal
public abstract class BaseTwoInputStreamOperatorWithStateRetention
	extends AbstractStreamOperator<BaseRow>
	implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow>, Triggerable<Object, VoidNamespace> {

	private static final long serialVersionUID = -5953921797477294258L;

	private static final String CLEANUP_TIMESTAMP = "cleanup-timestamp";
	private static final String TIMERS_STATE_NAME = "timers";

	private final long minRetentionTime;
	private final long maxRetentionTime;
	protected final boolean stateCleaningEnabled;

	private transient ValueState<Long> latestRegisteredCleanupTimer;
	private transient SimpleTimerService timerService;

	protected BaseTwoInputStreamOperatorWithStateRetention(long minRetentionTime, long maxRetentionTime) {
		this.minRetentionTime = minRetentionTime;
		this.maxRetentionTime = maxRetentionTime;
		this.stateCleaningEnabled = minRetentionTime > 1;
	}

	@Override
	public void open() throws Exception {
		initializeTimerService();

		if (stateCleaningEnabled) {
			ValueStateDescriptor<Long> cleanupStateDescriptor =
				new ValueStateDescriptor<>(CLEANUP_TIMESTAMP, Types.LONG);
			latestRegisteredCleanupTimer = getRuntimeContext().getState(cleanupStateDescriptor);
		}
	}

	private void initializeTimerService() {
		InternalTimerService<VoidNamespace> internalTimerService = getInternalTimerService(
			TIMERS_STATE_NAME,
			VoidNamespaceSerializer.INSTANCE,
			this);

		timerService = new SimpleTimerService(internalTimerService);
	}

	/**
	 * If the user has specified a {@code minRetentionTime} and {@code maxRetentionTime}, this
	 * method registers a cleanup timer for {@code currentProcessingTime + minRetentionTime}.
	 *
	 * <p>When this timer fires, the {@link #cleanupState(long)} method is called.
	 */
	protected void registerProcessingCleanupTimer() throws IOException {
		if (stateCleaningEnabled) {
			long currentProcessingTime = timerService.currentProcessingTime();
			Optional<Long> currentCleanupTime = Optional.ofNullable(latestRegisteredCleanupTimer.value());

			if (!currentCleanupTime.isPresent()
				|| (currentProcessingTime + minRetentionTime) > currentCleanupTime.get()) {

				updateCleanupTimer(currentProcessingTime, currentCleanupTime);
			}
		}
	}

	private void updateCleanupTimer(long currentProcessingTime, Optional<Long> currentCleanupTime) throws IOException {
		currentCleanupTime.ifPresent(aLong -> timerService.deleteProcessingTimeTimer(aLong));

		long newCleanupTime = currentProcessingTime + maxRetentionTime;
		timerService.registerProcessingTimeTimer(newCleanupTime);
		latestRegisteredCleanupTimer.update(newCleanupTime);
	}

	protected void cleanupLastTimer() throws IOException {
		if (stateCleaningEnabled) {
			Optional<Long> currentCleanupTime = Optional.ofNullable(latestRegisteredCleanupTimer.value());
			if (currentCleanupTime.isPresent()) {
				latestRegisteredCleanupTimer.clear();
				timerService.deleteProcessingTimeTimer(currentCleanupTime.get());
			}
		}
	}

	/**
	 * The users of this class are not allowed to use processing time timers.
	 * See class javadoc.
	 */
	@Override
	public final void onProcessingTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
		if (stateCleaningEnabled) {
			long timerTime = timer.getTimestamp();
			Long cleanupTime = latestRegisteredCleanupTimer.value();

			if (cleanupTime != null && cleanupTime == timerTime) {
				cleanupState(cleanupTime);
				latestRegisteredCleanupTimer.clear();
			}
		}
	}

	// ----------------- Abstract Methods -----------------

	/**
	 * The method to be called when a cleanup timer fires.
	 * @param time The timestamp of the fired timer.
	 */
	public abstract void cleanupState(long time);
}
