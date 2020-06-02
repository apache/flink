package org.apache.flink.api.common.eventtime;

/**
 * A {@link WatermarkStrategy} that overrides the {@link TimestampAssigner} of the given base {@link
 * WatermarkStrategy}.
 */
final class WatermarkStrategyWithTimestampAssigner<T> implements WatermarkStrategy<T> {

	private static final long serialVersionUID = 1L;

	private final WatermarkStrategy<T> baseStrategy;
	private final TimestampAssignerSupplier<T> timestampAssigner;

	WatermarkStrategyWithTimestampAssigner(
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
