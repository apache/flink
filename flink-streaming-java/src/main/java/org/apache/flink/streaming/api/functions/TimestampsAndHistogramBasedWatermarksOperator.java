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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This operator keeps statistics about the lateness observed on the stream of events received, and
 * emits watermarks that take this into account. More precisely, the user specifies the duration of a
 * sampling period and that of the interval between the end of a sampling period and the start of the
 * next, and a percentage referring to the <i>late</i> data that she wants to be covered, i.e. considered
 * non-late.
 *
 * Given this information, <b>during the sampling period</b> the operator keeps a per-second lateness histogram,
 * i.e. a histogram showing how may elements were 0, 1, 2... seconds late, and the maximum (event-time)
 * timestamp seen so far. When <b>the sampling period ends</b>, we compute the minimum lateness that covers
 * the user-specified percentage of data, and we keep this value. This value is not updated till the <b>end</b>
 * of the next sampling period.
 *
 * In the meantime, whenever a watermark is to be emitted, its timestamp will be the maximum seen (event-time)
 * timestamp - the previously computed lateness.
 * */
public class TimestampsAndHistogramBasedWatermarksOperator<T> extends AbstractStreamOperator<T>
	implements Triggerable, OneInputStreamOperator<T, T> {

	private static final long serialVersionUID = 1L;

	/** The set of pending sampling timers. */
	private Set<Long> samplingTimers = new HashSet<>();

	/** The set of pending watermark timers. */
	private Set<Long> watermarkTimers = new HashSet<>();

	/**
	 * A map holding the histogram. Currently we assume
	 * buckets of 2^10 milliseconds, which is roughly a second.
	 */
	private final Map<Integer, Integer> histBuckets = new TreeMap<>();

	private long lastEmittedWatermark = Long.MIN_VALUE;
	private long allowedLateness = 0;
	private long currentMaxSeenTimestamp = 0;

	private boolean inSampling = false;
	private long watermarkInterval = -1L;
	private int noOfEvents = 0;

	// User-defined parameters.

	private final double percentile;
	private final long interSamplingInterval;
	private final long samplingPeriod;
	private final AssignerWithPeriodicWatermarks<T> assigner;

	public TimestampsAndHistogramBasedWatermarksOperator(long nonSamplingPeriod,
														long samplingPeriod,
														double percent,
														AssignerWithPeriodicWatermarks<T> assigner) {

		if(nonSamplingPeriod < 0 || samplingPeriod < 0 || percent < 0) {
			throw new RuntimeException("Tried to set: " +
				"samplingPeriod=" + samplingPeriod + ", " +
				"interSamplingInterval=" + nonSamplingPeriod + ", and " +
				"coveragePercentila=" + percent + ".\n" +
				"These parameters cannot be negative numbers.");
		}

		if(assigner == null) {
			throw new RuntimeException("The timestampAssigner cannot be null");
		}

		this.samplingPeriod = samplingPeriod;
		this.interSamplingInterval = nonSamplingPeriod;
		this.percentile = percent;
		this.assigner = assigner;

		restoreToInit();
	}

	@Override
	public void open() throws Exception {
		super.open();
		setNextSamplingTimer(inSampling);
		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
		if (watermarkInterval > 0) {
			setNextWatermarkTimer();
		}
	}

	@Override
	public void trigger(long timestamp) throws Exception {
		if(samplingTimers.contains(timestamp)) {
			processSamplingTimer(timestamp);
		}
		if(watermarkTimers.contains(timestamp)) {
			processWatermarkTimer(timestamp);
		}
	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		final long newTimestamp = assigner.extractTimestamp(element.getValue(),
			element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

		long lateness = currentMaxSeenTimestamp - newTimestamp;
		if(lateness < 0) {
			lateness = 0;
			currentMaxSeenTimestamp = newTimestamp;
		}

		if(inSampling) {
			int bucket = computeBucket(lateness);
			Integer counter = histBuckets.get(bucket);
			if(counter == null) {
				counter = 0;
			}
			histBuckets.put(bucket, counter + 1);
			noOfEvents++;
		}

		output.collect(element.replace(element.getValue(), newTimestamp));
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// We generally ignore watermarks, as we emit our own.
		// The only exception is if we receive a Long.MAX_VALUE watermark.
		// In this case we forward it, as it signals the end of input.
		if (mark.getTimestamp() == Long.MAX_VALUE && lastEmittedWatermark != Long.MAX_VALUE) {
			lastEmittedWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		// emit a final watermark
		Watermark newWatermark = getCurrentWatermark();
		if (newWatermark != null && newWatermark.getTimestamp() > lastEmittedWatermark) {
			lastEmittedWatermark = newWatermark.getTimestamp();
			// emit watermark
			output.emitWatermark(newWatermark);
		}
		this.samplingTimers.clear();
		this.watermarkTimers.clear();
		this.histBuckets.clear();
		this.allowedLateness = 0;
		restoreToInit();
	}

	/**
	 * Registers a timer related to the start or the end of a sampling period.
	 * The timer can be either to start or to stop a sampling period. This is specified
	 * with the {@code timerToStartSampling} flag. Based on this flag, this {@code inSampling}
	 * flag is set to {@code true} or {@code false}, and a timer for the beginning or end of a
	 * sampling period is added. The time for the next trigger to fire adjusts accordingly.
	 * @param timerToStartSampling A flag indicating if the timer to register is to signal the
	 *                      start of the next sampling period, or the end of the current one.
	 */
	private void setNextSamplingTimer(boolean timerToStartSampling) {
		this.inSampling = !timerToStartSampling;
		long timeToNextSamplingTimer = System.currentTimeMillis() +
			(timerToStartSampling ? interSamplingInterval : samplingPeriod);
		samplingTimers.add(timeToNextSamplingTimer);
		registerTimer(timeToNextSamplingTimer, this);
	}

	/**
	 * Sets the next watermark timer.
	 */
	private void setNextWatermarkTimer() {
		long timeToNextWatermark = System.currentTimeMillis() + watermarkInterval;
		this.watermarkTimers.add(timeToNextWatermark);
		registerTimer(timeToNextWatermark, this);
	}

	/**
	 * Removes the timestamp from the list of pending sampling timers, adds the next one (to stop
	 * the new or start the next sampling period), and if we transitioned from a sampling period
	 * to a non-sampling one, then it updates the {@link this#allowedLateness} and resets all the
	 * remaining book-keeping data structures to their initial values.
	 *
	 * @param timestamp the timestamp of the timer that fired.
	 * */
	private void processSamplingTimer(long timestamp) {
		samplingTimers.remove(timestamp);
		setNextSamplingTimer(inSampling);
		if(!inSampling) {
			// we transitioned from sampling to a non-sampling period
			// so update the allowed lateness to reflect the new data,
			// and set everything else its initial state
			updateAllowedLateness();
			restoreToInit();
		}
	}

	/**
	 * Computes the {@link this#allowedLateness} for the next {@code non-sampling + sampling}
	 * period of time, based on the currently existing histogram.
	 */
	private void updateAllowedLateness() {
		int toFind = (int) (percentile * noOfEvents);
		int found = 0;
		for(Integer lateness: histBuckets.keySet()) {
			found += histBuckets.get(lateness);
			if(found >= toFind) {
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
	 * @param lateness how late the element was, in milliseconds.
	 * @return the bucket it should be registered in.
	 */
	private int computeBucket(long lateness) {
		if(lateness < 0) {
			throw new RuntimeException("Lateness cannot be a negative number.");
		}
		return (int) (lateness >>> 10);
	}

	/**
	 * Removes the timestamp from the list of pending watermark timers, emits the
	 * new watermark, and sets the timer for the next one.
	 */
	private void processWatermarkTimer(long timestamp) {
		watermarkTimers.remove(timestamp);

		Watermark newWatermark = getCurrentWatermark();
		if (newWatermark != null && newWatermark.getTimestamp() > lastEmittedWatermark) {
			lastEmittedWatermark = newWatermark.getTimestamp();
			output.emitWatermark(newWatermark);
		}
		setNextWatermarkTimer();
	}

	private Watermark getCurrentWatermark() {
		Watermark wm = assigner.getCurrentWatermark();
		long nextWmTimestamp = currentMaxSeenTimestamp - allowedLateness;
		return wm == null ? null : new Watermark(nextWmTimestamp);
	}

	private void restoreToInit() {
		noOfEvents = 0;
		histBuckets.clear();
	}
}
