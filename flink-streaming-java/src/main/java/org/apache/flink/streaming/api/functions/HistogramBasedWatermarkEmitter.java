/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.Triggerable;

import java.util.Map;
import java.util.TreeMap;

/**
 * When processing a stream, and for the lateness to be as low as possible, the watermark should
 * be as close to processing time as possible. Unfortunately, in streams with late data, i.e.
 * data with timestamps smaller than the last received watermark, this means that a number of elements
 * will be discarded due to lateness. In order to avoid this, this extractor periodically samples the
 * timestamps of the elements in a stream and keeps a histogram of the observed lateness. Based on this
 * histogram, it sets the watermark lateness to the lowest possible value that, at the same time,
 * guarantees that a user-specified percentage of the elements in the stream are covered and not
 * dropped due to lateness.
 *
 * <p>More precisely, the user specifies i) the duration of the sampling period, ii) that of the interval
 * between the end of a sampling period and the start of the next one, and iii) the percentage referring
 * of elements in the stream (late and non-late) that she wants to be covered, i.e. considered non-late.
 * Given this information, <b>during the sampling period</b> the extractor keeps a per-second lateness
 * histogram, i.e. a histogram showing how may elements were 0, 1, 2... seconds late, and the maximum
 * (event-time) timestamp seen so far. When <b>the sampling period ends</b>, it computes the minimum
 * lateness that covers the user-specified percentage of data, and whenever a watermark is emitted, its
 * timestamp is the maximum (event-time) timestamp seen up to that point in the stream, minus the
 * previously computed value. This value is not updated till the <b>end</b> of the next sampling period.
 * */
public abstract class HistogramBasedWatermarkEmitter<T> extends AbstractRichFunction
	implements AssignerWithPeriodicWatermarks<T>, Triggerable {

	private static final long serialVersionUID = 1L;

	private boolean testing;

	/**
	 * A map holding the histogram. Currently we assume
	 * buckets of 2^10 milliseconds, which is roughly a second.
	 */
	private final Map<Integer, Integer> histBuckets = new TreeMap<>();

	/**
	 * The allowed lateness, i.e. the amount of time the emitted watermark
	 * will lag behind the maximum timestamp seen so far.
	 */
	private long allowedLateness = 0;

	/** The maximum timestamp seen so far (in event-time). */
	private long currentMaxSeenTimestamp = Long.MIN_VALUE;

	/** A flag indicating if we are in a sampling period or not. */
	private boolean inSampling = false;

	/** The total number of events seen in the present sampling period. */
	private int noOfEvents = 0;

	// User-defined parameters.

	private final double percentile;
	private final long nonSamplingInterval;
	private final long samplingPeriod;

	/**
	 * A best-effort periodic watermark emitter. For more details see {@link HistogramBasedWatermarkEmitter}.
	 * @param samplingPeriod
	 *             the duration of the sampling.
	 * @param nonSamplingPeriod
	 * 				the interval between the end of a sampling and the
	 * 				beginning of the next one.
	 * @param percent
	 * 				the percentage of late and non-late elements in the stream
	 * 				to cover as non-late.
	 * */
	public HistogramBasedWatermarkEmitter(Time samplingPeriod,
										Time nonSamplingPeriod,
										double percent) {

		if (samplingPeriod == null || nonSamplingPeriod == null) {
			throw new RuntimeException("Tried to set: " +
				"samplingPeriod=" + samplingPeriod + ", " +
				"interSamplingInterval=" + nonSamplingPeriod + ".\n" +
				"These parameters cannot be null.");
		}

		if (percent < 0) {
			throw new RuntimeException("The percentage of data covered cannot be negative.");
		}

		this.samplingPeriod = samplingPeriod.toMilliseconds();
		this.nonSamplingInterval = nonSamplingPeriod.toMilliseconds();
		this.percentile = percent;
		restoreToInit();
	}

	public long getSamplingPeriodDurationInMillis() {
		return samplingPeriod;
	}

	public long getNonSamplingIntervalDurationInMillis() {
		return nonSamplingInterval;
	}

	public double getCoveragePercentile() {
		return this.percentile;
	}

	/**
	 * Extracts the timestamp from a record in the stream.
	 * @param element The element from which to extract the timestamp.
	 */
	public abstract long extractTimestamp(T element);

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		inSampling = true;
		setNextSamplingTimer(true);
	}

	@Override
	public Watermark getCurrentWatermark() {

		// we do not risk overflow because we always subtract from the currentMaxSeenTimestamp
		// we do not risk underflow because the allowed lateness will always be smaller than the
		// currentMaxSeenTimestamp and bigger than Long.MIN_VALUE
		long nextWmTimestamp = currentMaxSeenTimestamp - allowedLateness;
		return new Watermark(nextWmTimestamp);
	}

	@Override
	public long extractTimestamp(T element, long previousElementTimestamp) {
		long newTimestamp = extractTimestamp(element);
		long lateness = currentMaxSeenTimestamp - newTimestamp;

		if (currentMaxSeenTimestamp == Long.MIN_VALUE || lateness <= 0) {
			lateness = 0;
			currentMaxSeenTimestamp = newTimestamp;
		}

		if (inSampling) {
			int bucket = computeBucket(lateness);
			Integer counter = histBuckets.get(bucket);
			if (counter == null) {
				counter = 0;
			}
			histBuckets.put(bucket, counter + 1);
			noOfEvents++;
		}
		return newTimestamp;
	}

	@Override
	public void trigger(long timestamp) throws Exception {
		processSamplingTimer();
	}

	/**
	 * Adds the next sampling timer and does the necessary processing, depending on if we
	 * transition from a sampling to a non-sampling period, or the other way round.
	 * More specifically, if we transition from a sampling period to a non-sampling one, then
	 * it updates the {@link #allowedLateness} and resets all the remaining book-keeping
	 * data structures to their initial values. The interval till the next timer depends
	 * on if we enter, or we exit from a sampling period.
	 */
	private void processSamplingTimer() {
		if (inSampling && !histBuckets.isEmpty()) {
			// we transitioned from sampling to a non-sampling period
			// so update the allowed lateness to reflect the new data,
			// and set everything else its initial state for the next
			// sampling period.
			updateAllowedLateness();
			restoreToInit();
		}
		inSampling = !inSampling;
		setNextSamplingTimer(inSampling);
	}

	/**
	 * Registers a timer related to the start or the end of a sampling period.
	 * The timer can be either to start or to stop a sampling period. This is specified
	 * with the {@code timerToStartSampling} flag. Based on this flag, this {@code inSampling}
	 * flag is set to {@code true} or {@code false}, and a timer for the beginning or end of a
	 * sampling period is added. The time for the next trigger to fire adjusts accordingly.
	 *
	 * @param enterSampling A flag indicating if the timer to register is to signal the
	 *                      start of the next sampling period, or the end of the current one.
	 */
	private void setNextSamplingTimer(boolean enterSampling) {
		long timeToNextSamplingTimer = System.currentTimeMillis() +
			(enterSampling ? samplingPeriod : nonSamplingInterval);
		if(!testing) {
			((StreamingRuntimeContext) getRuntimeContext()).registerTimer(timeToNextSamplingTimer, this);
		}
	}

	/**
	 * Computes the {@code allowedLateness} based on the
	 * previously collected histogram. This is updated <i>after</i>
	 * the end of a sampling period, and stays fixed for the next
	 * {@code non-sampling + sampling} units of time.
	 */
	private void updateAllowedLateness() {
		int toFind = (int) (percentile * noOfEvents);
		int found = 0;
		for (Integer lateness : histBuckets.keySet()) {
			found += histBuckets.get(lateness);
			if (found >= toFind) {
				allowedLateness = lateness << 10;
				return;
			}
		}
	}

	/**
	 * Computes the bucket in the histogram that this statistic belongs to.
	 * If the element was not late, then this statistic is added to bucket 0.
	 * In other case it is added to the bucket that corresponds to its seconds of
	 * latency.
	 *
	 * @param lateness how late the element was, in milliseconds.
	 * @return the bucket it should be registered to.
	 */
	private int computeBucket(long lateness) {
		if (lateness < 0) {
			throw new RuntimeException("Lateness cannot be a negative number.");
		}
		return (int) (lateness >>> 10);
	}

	@Override
	public void close() throws Exception {
		super.close();
		this.histBuckets.clear();
		this.allowedLateness = 0;
		this.noOfEvents = 0;
		restoreToInit();
	}

	private void restoreToInit() {
		noOfEvents = 0;
		histBuckets.clear();
	}
}
