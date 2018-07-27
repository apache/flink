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

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.Serializable;

import static org.apache.flink.api.common.state.StateTtlConfiguration.TtlStateVisibility.NeverReturnExpired;
import static org.apache.flink.api.common.state.StateTtlConfiguration.TtlTimeCharacteristic.ProcessingTime;
import static org.apache.flink.api.common.state.StateTtlConfiguration.TtlUpdateType.OnCreateAndWrite;

/**
 * Configuration of state TTL logic.
 */
public class StateTtlConfiguration implements Serializable {

	private static final long serialVersionUID = -7592693245044289793L;

	public static final StateTtlConfiguration DISABLED =
		newBuilder(Time.milliseconds(Long.MAX_VALUE)).setTtlUpdateType(TtlUpdateType.Disabled).build();

	/**
	 * This option value configures when to update last access timestamp which prolongs state TTL.
	 */
	public enum TtlUpdateType {
		/** TTL is disabled. State does not expire. */
		Disabled,
		/** Last access timestamp is initialised when state is created and updated on every write operation. */
		OnCreateAndWrite,
		/** The same as <code>OnCreateAndWrite</code> but also updated on read. */
		OnReadAndWrite
	}

	/**
	 * This option configures whether expired user value can be returned or not.
	 */
	public enum TtlStateVisibility {
		/** Return expired user value if it is not cleaned up yet. */
		ReturnExpiredIfNotCleanedUp,
		/** Never return expired user value. */
		NeverReturnExpired
	}

	/**
	 * This option configures time scale to use for ttl.
	 */
	public enum TtlTimeCharacteristic {
		/** Processing time, see also <code>TimeCharacteristic.ProcessingTime</code>. */
		ProcessingTime
	}

	private final TtlUpdateType ttlUpdateType;
	private final TtlStateVisibility stateVisibility;
	private final TtlTimeCharacteristic timeCharacteristic;
	private final Time ttl;
	private final CleanupStrategies cleanupStrategies;

	private StateTtlConfiguration(
		TtlUpdateType ttlUpdateType,
		TtlStateVisibility stateVisibility,
		TtlTimeCharacteristic timeCharacteristic,
		Time ttl,
		CleanupStrategies cleanupStrategies) {
		this.ttlUpdateType = Preconditions.checkNotNull(ttlUpdateType);
		this.stateVisibility = Preconditions.checkNotNull(stateVisibility);
		this.timeCharacteristic = Preconditions.checkNotNull(timeCharacteristic);
		this.ttl = Preconditions.checkNotNull(ttl);
		this.cleanupStrategies = cleanupStrategies;
		Preconditions.checkArgument(ttl.toMilliseconds() > 0,
			"TTL is expected to be positive");
	}

	@Nonnull
	public TtlUpdateType getTtlUpdateType() {
		return ttlUpdateType;
	}

	@Nonnull
	public TtlStateVisibility getStateVisibility() {
		return stateVisibility;
	}

	@Nonnull
	public Time getTtl() {
		return ttl;
	}

	@Nonnull
	public TtlTimeCharacteristic getTimeCharacteristic() {
		return timeCharacteristic;
	}

	public boolean isEnabled() {
		return ttlUpdateType != TtlUpdateType.Disabled;
	}

	@Nonnull
	public CleanupStrategies getCleanupStrategies() {
		return cleanupStrategies;
	}

	@Override
	public String toString() {
		return "StateTtlConfiguration{" +
			"ttlUpdateType=" + ttlUpdateType +
			", stateVisibility=" + stateVisibility +
			", timeCharacteristic=" + timeCharacteristic +
			", ttl=" + ttl +
			'}';
	}

	@Nonnull
	public static Builder newBuilder(@Nonnull Time ttl) {
		return new Builder(ttl);
	}

	/**
	 * Builder for the {@link StateTtlConfiguration}.
	 */
	public static class Builder {

		private TtlUpdateType ttlUpdateType = OnCreateAndWrite;
		private TtlStateVisibility stateVisibility = NeverReturnExpired;
		private TtlTimeCharacteristic timeCharacteristic = ProcessingTime;
		private Time ttl;
		private CleanupStrategies cleanupStrategies = new CleanupStrategies();

		public Builder(@Nonnull Time ttl) {
			this.ttl = ttl;
		}

		/**
		 * Sets the ttl update type.
		 *
		 * @param ttlUpdateType The ttl update type configures when to update last access timestamp which prolongs state TTL.
		 */
		@Nonnull
		public Builder setTtlUpdateType(TtlUpdateType ttlUpdateType) {
			this.ttlUpdateType = ttlUpdateType;
			return this;
		}

		@Nonnull
		public Builder updateTtlOnCreateAndWrite() {
			return setTtlUpdateType(TtlUpdateType.OnCreateAndWrite);
		}

		@Nonnull
		public Builder updateTtlOnReadAndWrite() {
			return setTtlUpdateType(TtlUpdateType.OnReadAndWrite);
		}

		/**
		 * Sets the state visibility.
		 *
		 * @param stateVisibility The state visibility configures whether expired user value can be returned or not.
		 */
		@Nonnull
		public Builder setStateVisibility(@Nonnull TtlStateVisibility stateVisibility) {
			this.stateVisibility = stateVisibility;
			return this;
		}

		@Nonnull
		public Builder returnExpiredIfNotCleanedUp() {
			return setStateVisibility(TtlStateVisibility.ReturnExpiredIfNotCleanedUp);
		}

		@Nonnull
		public Builder neverReturnExpired() {
			return setStateVisibility(TtlStateVisibility.NeverReturnExpired);
		}

		/**
		 * Sets the time characteristic.
		 *
		 * @param timeCharacteristic The time characteristic configures time scale to use for ttl.
		 */
		@Nonnull
		public Builder setTimeCharacteristic(@Nonnull TtlTimeCharacteristic timeCharacteristic) {
			this.timeCharacteristic = timeCharacteristic;
			return this;
		}

		@Nonnull
		public Builder useProcessingTime() {
			return setTimeCharacteristic(TtlTimeCharacteristic.ProcessingTime);
		}

		/** Cleanup expired state in full snapshot on checkpoint. */
		@Nonnull
		public Builder cleanupFullSnapshot() {
			cleanupStrategies.strategies[CleanupStrategies.Strategies.FULL_STATE_SCAN_SNAPSHOT.ordinal()] =
				new CleanupStrategies.CleanupStrategy() {  };
			return this;
		}

		/**
		 * Sets the ttl time.
		 * @param ttl The ttl time.
		 */
		@Nonnull
		public Builder setTtl(@Nonnull Time ttl) {
			this.ttl = ttl;
			return this;
		}

		@Nonnull
		public StateTtlConfiguration build() {
			return new StateTtlConfiguration(
				ttlUpdateType,
				stateVisibility,
				timeCharacteristic,
				ttl,
				cleanupStrategies);
		}
	}

	/**
	 * TTL cleanup strategies.
	 *
	 * <p>This class configures when to cleanup expired state with TTL.
	 * By default, state is always cleaned up on explicit read access if found expired.
	 * Currently cleanup of state full snapshot can be additionally activated.
	 */
	public static class CleanupStrategies implements Serializable {
		/** Fixed strategies ordinals in {@code strategies} config field. */
		enum Strategies {
			FULL_STATE_SCAN_SNAPSHOT
		}

		/** Base interface for cleanup strategies configurations. */
		interface CleanupStrategy extends Serializable {

		}

		final CleanupStrategy[] strategies = new CleanupStrategy[Strategies.values().length];

		public boolean inFullSnapshot() {
			return strategies[Strategies.FULL_STATE_SCAN_SNAPSHOT.ordinal()] != null;
		}
	}
}
