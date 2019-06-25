package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@code WindowStagger} staggers offset in runtime for each window assignment.
 */
public enum WindowStagger {
	/**
	 * Default mode,  all panes fire at the same time across all partitions.
	 */
	ALIGNED {
		@Override
		public long getStaggerOffset(
			final long currentProcessingTime,
			final long size) {
			return 0L;
		}
	},

	/**
	 * Stagger offset is sampled from uniform distribution U(0, WindowSize) when first event ingested in the partitioned operator.
	 */
	RANDOM {
		@Override
		public long getStaggerOffset(
			final long currentProcessingTime,
			final long size) {
			return (long) (ThreadLocalRandom.current().nextDouble() * size);
		}
	},

	/**
	 * Stagger offset is the ingestion delay in processing time, which is the difference between first event ingestion time and its corresponding processing window start time
	 * in the partitioned operator. In other words, each partitioned window starts when its first pane created.
	 */
	NATURAL {
		@Override
		public long getStaggerOffset(
			final long currentProcessingTime,
			final long size) {
			final long currentProcessingWindowStart = TimeWindow.getWindowStartWithOffset(currentProcessingTime, 0, size);
			return Math.max(0, currentProcessingTime - currentProcessingWindowStart);
		}
	};
	public abstract long getStaggerOffset(final long currentProcessingTime, final long size);
}
