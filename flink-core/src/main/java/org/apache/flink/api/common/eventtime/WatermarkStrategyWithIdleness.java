package org.apache.flink.api.common.eventtime;

import java.time.Duration;

/**
 * A {@link WatermarkStrategy} that adds idleness detection on top of the wrapped strategy.
 */
final class WatermarkStrategyWithIdleness<T> implements WatermarkStrategy<T> {

	private static final long serialVersionUID = 1L;

	private final WatermarkStrategy<T> baseStrategy;
	private final Duration idlenessTimeout;

	WatermarkStrategyWithIdleness(WatermarkStrategy<T> baseStrategy, Duration idlenessTimeout) {
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
