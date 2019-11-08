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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * WatermarkStrategies is a simply way to build a {@link WatermarkStrategy} by configuring
 * common strategies.
 */
public final class WatermarkStrategies {

	/** The base strategy for watermark generation. Starting point, is always set. */
	private final WatermarkStrategy<?> baseStrategy;

	/** Optional idle timeout for watermarks. */
	@Nullable
	private Duration idleTimeout;

	private WatermarkStrategies(WatermarkStrategy<?> baseStrategy) {
		this.baseStrategy = baseStrategy;
	}

	// ------------------------------------------------------------------------
	//  builder methods
	// ------------------------------------------------------------------------

	/**
	 * Add an idle timeout to the watermark strategy.
	 * If no records flow in a partition of a stream for that amount of time, then that partition
	 * is considered "idle" and will not hold back the progress of watermarks in downstream operators.
	 *
	 * <p>Idleness can be important if some partitions have little data and might not have events during
	 * some periods. Without idleness, these streams can stall the overall event time progress of the
	 * application.
	 */
	public WatermarkStrategies withIdleness(Duration idleTimeout) {
		checkNotNull(idleTimeout, "idleTimeout");
		checkArgument(!(idleTimeout.isZero() || idleTimeout.isNegative()), "idleTimeout must be greater than zero");
		this.idleTimeout = idleTimeout;
		return this;
	}

	/**
	 * Build the watermark strategy.
	 */
	public <T> WatermarkStrategy<T> build() {
		@SuppressWarnings("unchecked")
		WatermarkStrategy<T> strategy = (WatermarkStrategy<T>) this.baseStrategy;

		if (idleTimeout != null) {
			strategy = new WithIdlenessStrategy<>(strategy, idleTimeout);
		}

		return strategy;
	}

	// ------------------------------------------------------------------------
	//  builder entry points
	// ------------------------------------------------------------------------

	/**
	 * Starts building a watermark strategy for situations with monotonously ascending
	 * timestamps.
	 *
	 * <p>The watermarks are generated periodically and tightly follow the latest
	 * timestamp in the data. The delay introduced by this strategy is mainly the periodic
	 * interval in which the watermarks are generated.
	 *
	 * @see AscendingTimestampsWatermarks
	 */
	public static WatermarkStrategies forMonotonousTimestamps() {
		return new WatermarkStrategies(AscendingTimestampsWatermarks::new);
	}

	/**
	 * Starts building a watermark strategy for situations where records are out of order, but
	 * you can place an upper bound on how far the events are out of order.
	 * An out-of-order bound B means that once the an event with timestamp T was encountered, no
	 * events older than {@code T - B} will follow any more.
	 *
	 * <p>The watermarks are generated periodically. The delay introduced by this watermark strategy
	 * is the periodic interval length, plus the out of orderness bound.
	 *
	 * @see BoundedOutOfOrdernessWatermarks
	 */
	public static WatermarkStrategies forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
		return new WatermarkStrategies(() -> new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness));
	}

	/**
	 * Starts building a watermark strategy based on an existing {@code WatermarkStrategy}.
	 */
	public static WatermarkStrategies forStrategy(WatermarkStrategy<?> strategy) {
		return new WatermarkStrategies(strategy);
	}

	/**
	 * Starts building a watermark strategy based on an existing {@code WatermarkGenerator}.
	 */
	public static <X extends WatermarkGenerator<?> & Serializable> WatermarkStrategies forGenerator(X generator) {
		@SuppressWarnings("unchecked")
		final WatermarkGenerator<Object> gen = (WatermarkGenerator<Object>) generator;
		return new WatermarkStrategies(new FromSerializedGeneratorStrategy<>(gen));
	}

	// ------------------------------------------------------------------------

	private static final class FromSerializedGeneratorStrategy<T> implements WatermarkStrategy<T> {
		private static final long serialVersionUID = 1L;

		private final WatermarkGenerator<T> generator;

		private FromSerializedGeneratorStrategy(WatermarkGenerator<T> generator) {
			this.generator = generator;
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator() {
			try {
				byte[] serialized = InstantiationUtil.serializeObject(generator);
				return InstantiationUtil.deserializeObject(serialized, generator.getClass().getClassLoader());
			}
			catch (Exception e) {
				throw new FlinkRuntimeException("Cannot clone watermark generator via serialization");
			}
		}
	}

	private static final class WithIdlenessStrategy<T> implements WatermarkStrategy<T> {
		private static final long serialVersionUID = 1L;

		private final WatermarkStrategy<T> baseStrategy;
		private final Duration idlenessTimeout;

		private WithIdlenessStrategy(WatermarkStrategy<T> baseStrategy, Duration idlenessTimeout) {
			this.baseStrategy = baseStrategy;
			this.idlenessTimeout = idlenessTimeout;
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator() {
			return new WatermarksWithIdleness<>(baseStrategy.createWatermarkGenerator(), idlenessTimeout);
		}
	}
}
