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

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;
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
public abstract class HistogramBasedWatermarkEmitter<T> implements AssignerWithPeriodicWatermarks<T> {

	private static final long serialVersionUID = 1L;

	/** The time this operator started sampling for the first time. */
	private long startTime = -1L;

	/**
	 * A map holding the histogram. Currently we assume
	 * buckets of 2^10 milliseconds, which is roughly a second.
	 */
	private final Map<Integer, Integer> histBuckets = new TreeMap<>();

	/**
	 * The allowed lateness, i.e. the amount of time the emitted watermark
	 * will lag behind the maximum timestamp seen so far.
	 * */
	private long allowedLateness = 0;

	/** The maximum timestamp seen so far (in event-time). */
	private long currentMaxSeenTimestamp = Long.MIN_VALUE;

	private long samplingRound = 0;
	private final long roundDuration;

	/** A flag indicating if we are in a sampling period or not. */
	private boolean inSampling = false;

	/** The total number of events seen in the present sampling period. */
	private int noOfEvents = 0;

	// User-defined parameters.

	private final double percentile;
	private final long nonSamplingInterval;
	private final long samplingPeriod;

	public HistogramBasedWatermarkEmitter(Time nonSamplingPeriod,
										Time samplingPeriod,
										double percent) {

		if(samplingPeriod == null || nonSamplingPeriod == null) {
			throw new RuntimeException("Tried to set: " +
				"samplingPeriod=" + samplingPeriod + ", " +
				"interSamplingInterval=" + nonSamplingPeriod + ".\n" +
				"These parameters cannot be null.");
		}

		if(percent < 0) {
			throw new RuntimeException("The percentage of data covered cannot be negative.");
		}

		this.samplingPeriod = samplingPeriod.toMilliseconds();
		this.nonSamplingInterval = nonSamplingPeriod.toMilliseconds();

		this.roundDuration = this.samplingPeriod + this.nonSamplingInterval;
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
	public Watermark getCurrentWatermark() {
		checkPeriod();

		// we do not risk overflow because we always subtract from the currentMaxSeenTimestamp
		// we do not risk underflow because the allowed lateness will always be smaller than the
		// currentMaxSeenTimestamp and bigger than Long.MIN_VALUE
		long nextWmTimestamp = currentMaxSeenTimestamp - allowedLateness;
		return new Watermark(nextWmTimestamp);
	}

	@Override
	public long extractTimestamp(T element, long previousElementTimestamp) {
		checkPeriod();
		long newTimestamp = extractTimestamp(element);
		long lateness = currentMaxSeenTimestamp - newTimestamp;

		if(currentMaxSeenTimestamp == Long.MIN_VALUE || lateness <= 0) {
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
		return newTimestamp;
	}

	/**
	 * Checks if we are currently in a sampling or non-sampling period and sets the
	 * {@code inSampling} flag. In the transition from a sampling to a non-sampling
	 * period, it updates the {@code allowedLateness} and resets all the remaining
	 * book-keeping data structures to their initial values.
	 */
	private void checkPeriod() {
		if(startTime == -1L) {
			// this is when the operator is executed for the first time.
			startTime = getCurrentSystemTime();
			inSampling = true;
		} else {
			boolean beforeSamplingFlag = inSampling;
			long beforeSamplingRound = samplingRound;
			long runningTime = getCurrentSystemTime() - startTime;

			this.samplingRound = runningTime / roundDuration;
			this.inSampling = runningTime % roundDuration <= samplingPeriod;

			if(beforeSamplingFlag && !inSampling ||
				(beforeSamplingFlag && inSampling && beforeSamplingRound != samplingRound)) {

				// we transitioned from sampling to a non-sampling period
				// so update the allowed lateness to reflect the new data,
				// and set everything else its initial state for the next
				// sampling period.
				updateAllowedLateness();
				restoreToInit();
			}
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
	 * @return the bucket it should be registered to.
	 */
	private int computeBucket(long lateness) {
		if(lateness < 0) {
			throw new RuntimeException("Lateness cannot be a negative number.");
		}
		return (int) (lateness >>> 10);
	}

	private void restoreToInit() {
		noOfEvents = 0;
		histBuckets.clear();
	}

	////////////			Testing code				///////////////

	private boolean testing = false;

	private long currentSystemTime = 0;

	public void setCurrentSystemTime(long millis) {
		if(!testing) {
			throw new RuntimeException("Only allowed to set your own time when testing.");
		} else if(millis < 0 || millis < currentSystemTime) {
			throw new RuntimeException("Time cannot be negative and should be ascending. " +
				"Prev=" + currentSystemTime + " Attempted=" + millis);
		}
		this.currentSystemTime = millis;
	}

	public long getCurrentSystemTime() {
		return testing ? currentSystemTime : System.currentTimeMillis();
	}
}
