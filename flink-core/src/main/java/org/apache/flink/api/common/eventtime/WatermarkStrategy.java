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

import org.apache.flink.annotation.Public;

import java.io.Serializable;
import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The WatermarkStrategy defines how to generate {@link Watermark}s in the stream sources. The
 * WatermarkStrategy is a builder/factory for the {@link WatermarkGenerator} that generates the
 * watermarks and the {@link TimestampAssigner} which assigns the internal timestamp of a record.
 *
 * <p>This interface is {@link Serializable} because watermark strategies may be shipped
 * to workers during distributed execution.
 */
@Public
public interface WatermarkStrategy<T> extends TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T> {

	/**
	 * Instantiates a {@link TimestampAssigner} for assigning timestamps according to this
	 * strategy.
	 */
	@Override
	default TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
		// By default, this is {@link RecordTimestampAssigner},
		// for cases where records come out of a source with valid timestamps, for example from Kafka.
		return new RecordTimestampAssigner<>();
	}

	/**
	 * Instantiates a WatermarkGenerator that generates watermarks according to this strategy.
	 */
	@Override
	WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);

	/**
	 * Creates a watermark strategy for situations with monotonously ascending timestamps.
	 *
	 * <p>The watermarks are generated periodically and tightly follow the latest
	 * timestamp in the data. The delay introduced by this strategy is mainly the periodic interval
	 * in which the watermarks are generated.
	 *
	 * @see AscendingTimestampsWatermarks
	 */
	static <T> WatermarkStrategy<T> forMonotonousTimestamps() {
		return new FromWatermarkGeneratorSupplier<>((ctx) -> new AscendingTimestampsWatermarks<>());
	}

	/**
	 * Creates a watermark strategy for situations where records are out of order, but you
	 * can place an upper bound on how far the events are out of order. An out-of-order bound B
	 * means that once the an event with timestamp T was encountered, no events older than {@code T
	 * - B} will follow any more.
	 *
	 * <p>The watermarks are generated periodically. The delay introduced by this watermark
	 * strategy
	 * is the periodic interval length, plus the out of orderness bound.
	 *
	 * @see BoundedOutOfOrdernessWatermarks
	 */
	static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
		return new FromWatermarkGeneratorSupplier<>(
				(ctx) -> new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness));
	}

	/**
	 * Creates a watermark strategy based on an existing {@link
	 * WatermarkGeneratorSupplier}.
	 */
	static <T> WatermarkStrategy<T> forGenerator(WatermarkGeneratorSupplier<T> generatorSupplier) {
		return new FromWatermarkGeneratorSupplier<>(generatorSupplier);
	}

	/**
	 * Creates a watermark strategy that generates no watermarks at all.
	 * This may be useful in scenarios that do pure processing-time based stream processing.
	 */
	public static <T> WatermarkStrategy<T> noWatermarks() {
		return new FromWatermarkGeneratorSupplier<>((ctx) -> new NoWatermarksGenerator<>());
	}

	/**
	 * Creates Adds the given {@link TimestampAssigner} (via a {@link TimestampAssignerSupplier}) to
	 * this {@link WatermarkStrategy}.
	 *
	 * <p>You can use this when a {@link TimestampAssigner} needs additional context, for example
	 * access to the metrics system.
	 *
	 * <pre>
	 * {@code WatermarkStrategy<Object> wmStrategy = WatermarkStrategy
	 *   .forMonotonousTimestamps()
	 *   .withTimestampAssigner((ctx) -> new MetricsReportingAssigner(ctx));
	 * }</pre>
	 */
	default WatermarkStrategy<T> withTimestampAssigner(TimestampAssignerSupplier<T> timestampAssigner) {
		checkNotNull(timestampAssigner, "timestampAssigner");
		return new WithTimestampAssigner<>(this, timestampAssigner);
	}

	/**
	 * Creates a new {@link WatermarkStrategy} with the {@link TimestampAssigner} overridden by the
	 * provided assigner.
	 *
	 * <p>You can use this in case you want to specify a {@link TimestampAssigner} via a lambda
	 * function.
	 *
	 * <pre>
	 * {@code WatermarkStrategy<CustomObject> wmStrategy = WatermarkStrategy
	 *   .forMonotonousTimestamps()
	 *   .withTimestampAssigner((event, timestamp) -> event.getTimestamp());
	 * }</pre>
	 */
	default WatermarkStrategy<T> withTimestampAssigner(SerializableTimestampAssigner<T> timestampAssigner) {
		checkNotNull(timestampAssigner, "timestampAssigner");
		return new WithTimestampAssigner<>(this, TimestampAssignerSupplier.of(timestampAssigner));
	}

	/**
	 * Add an idle timeout to the watermark strategy. If no records flow in a partition of a stream
	 * for that amount of time, then that partition is considered "idle" and will not hold back the
	 * progress of watermarks in downstream operators.
	 *
	 * <p>Idleness can be important if some partitions have little data and might not have events
	 * during
	 * some periods. Without idleness, these streams can stall the overall event time progress of
	 * the application.
	 */
	default WatermarkStrategy<T> withIdleness(Duration idleTimeout) {
		checkNotNull(idleTimeout, "idleTimeout");
		checkArgument(!(idleTimeout.isZero() || idleTimeout.isNegative()),
				"idleTimeout must be greater than zero");
		return new WithIdlenessStrategy<>(this, idleTimeout);
	}

	/**
	 * A {@link WatermarkStrategy} that adds idleness detection on top of the wrapped strategy.
	 */
	class WithIdlenessStrategy<T> implements WatermarkStrategy<T> {

		private static final long serialVersionUID = 1L;

		private final WatermarkStrategy<T> baseStrategy;
		private final Duration idlenessTimeout;

		private WithIdlenessStrategy(WatermarkStrategy<T> baseStrategy, Duration idlenessTimeout) {
			this.baseStrategy = baseStrategy;
			this.idlenessTimeout = idlenessTimeout;
		}

		@Override
		public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
			return baseStrategy.createTimestampAssigner(context);
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
			return new WatermarksWithIdleness<>(baseStrategy.createWatermarkGenerator(context),
					idlenessTimeout);
		}
	}

	/**
	 * A {@link WatermarkStrategy} that overrides the {@link TimestampAssigner} of the given base
	 * {@link WatermarkStrategy}.
	 */
	class WithTimestampAssigner<T> implements WatermarkStrategy<T> {

		private static final long serialVersionUID = 1L;

		private final WatermarkStrategy<T> baseStrategy;
		private final TimestampAssignerSupplier<T> timestampAssigner;

		private WithTimestampAssigner(
				WatermarkStrategy<T> baseStrategy,
				TimestampAssignerSupplier<T> timestampAssigner) {
			this.baseStrategy = baseStrategy;
			this.timestampAssigner = timestampAssigner;
		}

		@Override
		public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
			return timestampAssigner.createTimestampAssigner(context);
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
			return baseStrategy.createWatermarkGenerator(context);
		}
	}

	/**
	 * A {@link WatermarkStrategy} that uses the wrapped {@link WatermarkGeneratorSupplier} in
	 * {@link #createWatermarkGenerator(WatermarkGeneratorSupplier.Context)}.
	 */
	class FromWatermarkGeneratorSupplier<T> implements WatermarkStrategy<T> {

		private static final long serialVersionUID = 1L;

		private final WatermarkGeneratorSupplier<T> generatorSupplier;

		private FromWatermarkGeneratorSupplier(WatermarkGeneratorSupplier<T> generatorSupplier) {
			this.generatorSupplier = generatorSupplier;
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
			return generatorSupplier.createWatermarkGenerator(context);
		}
	}
}
