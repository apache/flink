package org.apache.flink.streaming.api.functions;

import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.watermark.Watermark;

public abstract class HistogramBasedWatermarkEmiter<T> implements AssignerWithPunctuatedWatermarks<T> {

	private static final long serialVersionUID = 1L;

	private final double percentile;
	private final int samplingPeriod;

	private int currentBucket = 0;

	private long initTimestamp = -1L;

	private int numberOfEvents = 0;
	private int[] counters;

	public HistogramBasedWatermarkEmiter(int period, double percent) {

		Preconditions.checkArgument(period >= 0,
			"The samplingPeriod parameter has to be a positive number.");
		Preconditions.checkArgument(percent >= 0.5,
			"The percentile of data at which to emit watermarks has to be > 0.5.");

		samplingPeriod = period;
		percentile = percent;

		counters = new int[samplingPeriod];
		for(int i = 0; i < counters.length; i++) {
			counters[i] = 0;
		}
	}

	/**
	 * Extracts the timestamp from the given element.
	 *
	 * @param element The element that the timestamp is extracted from.
	 * @return The new timestamp.
	 */
	public abstract long extractTimestamp(T element);

	@Override
	public long extractTimestamp(T element, long previousElementTimestamp) {
		long timestamp = extractTimestamp(element);

		int bucket = computeBucket(timestamp);
		if(bucket < currentBucket) {
			restoreToInit(timestamp);
		}
		numberOfEvents++;
		counters[bucket]++;
		currentBucket = bucket;
		return timestamp;
	}

	@Override
	public Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp) {
		int elementsToFind = ????;
		return (numberOfEvents >= elementsToFind) ? new Watermark(extractedTimestamp) : null;
	}

	private void restoreToInit(long timestamp) {
		initTimestamp = timestamp;
		numberOfEvents = 0;
		for(int i = 0; i < counters.length; i++) {
			counters[i] = 0;
		}
	}

	private int computeBucket(long timestamp) {
		if(initTimestamp == -1) {
			initTimestamp = System.currentTimeMillis();
		}
		return (int) ((timestamp - initTimestamp) >>> 10) + currentBucket;
	}
}
