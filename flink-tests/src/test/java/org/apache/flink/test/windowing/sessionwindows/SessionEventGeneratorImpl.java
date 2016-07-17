/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.windowing.sessionwindows;

import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of EventGenerator that generates timely and late (in-lateness and after-lateness) events for
 * a single session.
 *
 * @param <K>
 * @param <E>
 */
public class SessionEventGeneratorImpl<K, E> implements EventGenerator<K, E> {

	/**
	 * Event timing w.r.t the global watermark
	 */
	public enum Timing {
		TIMELY, IN_LATENESS, AFTER_LATENESS
	}

	// pseudo random engine
	private final LongRandomGenerator randomGenerator;

	// configuration for the generated session stream
	private final SessionStreamConfiguration<K, E> configuration;

	// precomputed timestamps for the timely events (could be a list of primitive longs)
	private final List<Long> orderedTimelyTimestamps;

	// the minimum timestamp for events emitted by this generator
	private final long minTimestamp;

	// the maximum timestamp for events emitted by this generator
	private final long maxTimestamp;

	// tracks how many events this generator has produced
	private int producedEventsCount;

	// sub-generator that capture the lifecycle w.r.t. the global watermark (timely, in lateness, after lateness)
	private EventGenerator<K, E> timingAwareEventGenerator;

	/**
	 * @param configuration   stream configuration
	 * @param randomGenerator random engine for the event generation
	 */
	public SessionEventGeneratorImpl(
			SessionStreamConfiguration<K, E> configuration, LongRandomGenerator randomGenerator) {
		Preconditions.checkNotNull(configuration);
		Preconditions.checkNotNull(randomGenerator);

		this.producedEventsCount = 0;
		this.configuration = configuration;
		this.randomGenerator = randomGenerator;

		//pre-compute and store all timestamps for the timely events in this session
		final int timelyEventsInSessionCount = configuration.getSessionConfiguration().getNumberOfTimelyEvents();
		this.orderedTimelyTimestamps = new ArrayList<>(timelyEventsInSessionCount);
		this.minTimestamp = configuration.getSessionConfiguration().getMinEventTimestamp();
		generateOrderedTimelyTimestamps(minTimestamp, timelyEventsInSessionCount);
		this.maxTimestamp = orderedTimelyTimestamps.get(orderedTimelyTimestamps.size() - 1);
		this.timingAwareEventGenerator = new TimelyGenerator();
	}

	/**
	 * @see EventGenerator
	 */
	@Override
	public boolean canProduceEventAtWatermark(long globalWatermark) {
		return timingAwareEventGenerator.canProduceEventAtWatermark(globalWatermark);
	}

	/**
	 * @see EventGenerator
	 */
	@Override
	public E generateEvent(long globalWatermark) {
		if (hasMoreEvents()) {
			++producedEventsCount;
			E event = timingAwareEventGenerator.generateEvent(globalWatermark);

			while (!timingAwareEventGenerator.hasMoreEvents()) {
				timingAwareEventGenerator = timingAwareEventGenerator.getNextGenerator(globalWatermark);
			}

			return event;
		} else {
			throw new IllegalStateException("All events exhausted");
		}
	}

	/**
	 * @see EventGenerator
	 */
	@Override
	public long getLocalWatermark() {
		return timingAwareEventGenerator.getLocalWatermark();
	}

	/**
	 * @see EventGenerator
	 */
	@Override
	public boolean hasMoreEvents() {
		return producedEventsCount < getAllEventsCount();
	}

	/**
	 * pre-computes and stores the timestamps for timely events in this session in a list (ordered)
	 *
	 * @param minTimestamp              the minimum event time in the session
	 * @param onTimeEventCountInSession the number of timestamps to generate
	 */
	private void generateOrderedTimelyTimestamps(long minTimestamp, int onTimeEventCountInSession) {
		long generatedTimestamp = minTimestamp;

		for (int i = 1; i < onTimeEventCountInSession; ++i) {
			orderedTimelyTimestamps.add(generatedTimestamp);
			generatedTimestamp += randomGenerator.randomLongBetween(0, getGap() - 1);
		}

		orderedTimelyTimestamps.add(generatedTimestamp);
	}

	/**
	 * @param eventTimestamp
	 * @param globalWatermark
	 * @param timing
	 * @return
	 */
	private E createEventFromTimestamp(long eventTimestamp, long globalWatermark, Timing timing) {
		return getEventFactory().createEvent(
				getKey(),
				getSessionId(),
				producedEventsCount,
				eventTimestamp,
				globalWatermark,
				timing);
	}

	/**
	 * @return
	 */
	private long generateTimelyTimestamp() {
		int chosenTimestampIndex = randomGenerator.choseRandomIndex(orderedTimelyTimestamps);
		// performance: consider that remove is an O(n) operation here, with n being the number of timely events but
		// this should not matter too much for a IT case
		return orderedTimelyTimestamps.remove(chosenTimestampIndex);
	}

	/**
	 * @return
	 */
	private long generateLateTimestamp() {
		return randomGenerator.randomLongBetween(minTimestamp, maxTimestamp + 1);
	}

	/**
	 * @param globalWatermark
	 * @return true if the session window for this session has already triggered at global watermark
	 */
	private boolean isTriggered(long globalWatermark) {
		return globalWatermark >= maxTimestamp + getGap() - 1;
	}

	/**
	 * @param globalWatermark
	 * @return true if all future generated events are after lateness w.r.t global watermark
	 */
	private boolean isAfterLateness(long globalWatermark) {
		return globalWatermark >= getAfterLatenessTimestamp();
	}

	/**
	 * @return timestamp of the watermark at events for this session are after the lateness
	 */
	private long getAfterLatenessTimestamp() {
		return getTriggerTimestamp() + getLateness();
	}

	/**
	 * @return timestamp of the watermark at which the window for this session will trigger
	 */
	private long getTriggerTimestamp() {
		return maxTimestamp + getGap() - 1;
	}

	@Override
	public K getKey() {
		return configuration.getSessionConfiguration().getKey();
	}

	private long getGap() {
		return configuration.getSessionConfiguration().getGap();
	}

	private long getLateness() {
		return configuration.getStreamConfiguration().getAllowedLateness();
	}

	private StreamEventFactory<K, E> getEventFactory() {
		return configuration.getSessionConfiguration().getEventFactory();
	}

	private int getSessionId() {
		return configuration.getSessionConfiguration().getSessionId();
	}

	private int getTimelyEventsCount() {
		return configuration.getSessionConfiguration().getNumberOfTimelyEvents();
	}

	private int getLateEventsCount() {
		return getTimelyEventsCount() + configuration.getStreamConfiguration().getLateEventsWithinLateness();
	}

	private int getAllEventsCount() {
		return getLateEventsCount() + configuration.getStreamConfiguration().getLateEventsAfterLateness();
	}

	private boolean hasMoreTimelyEvents() {
		return !orderedTimelyTimestamps.isEmpty();
	}

	private boolean hasMoreInLatenessEvents() {
		return producedEventsCount < getLateEventsCount();
	}

	/**
	 * @see EventGenerator
	 */
	@Override
	public EventGenerator<K, E> getNextGenerator(long globalWatermark) {
		StreamConfiguration streamConfiguration = configuration.getStreamConfiguration();
		SessionConfiguration<K, E> sessionConfiguration = configuration.getSessionConfiguration();

		//compute the start timestamp for the next session
		long maxAdditionalGap = streamConfiguration.getMaxAdditionalSessionGap();
		long nextStartTime = Math.max(
				getAfterLatenessTimestamp() + randomGenerator.randomLongBetween(0, maxAdditionalGap),
				globalWatermark);

		sessionConfiguration = configuration.getSessionConfiguration().getFollowupSessionConfiguration(nextStartTime);
		SessionStreamConfiguration<K, E> sessionStreamConfiguration =
				new SessionStreamConfiguration<>(sessionConfiguration, streamConfiguration);

		return new SessionEventGeneratorImpl<>(sessionStreamConfiguration, randomGenerator);
	}

	public long getMinTimestamp() {
		return minTimestamp;
	}

	public long getMaxTimestamp() {
		return maxTimestamp;
	}


	abstract class AbstractEventGenerator implements EventGenerator<K, E> {
		@Override
		public K getKey() {
			return configuration.getSessionConfiguration().getKey();
		}
	}

	/**
	 * internal generator delegate for producing session events that are timely
	 */
	class TimelyGenerator extends AbstractEventGenerator {

		@Override
		public E generateEvent(long globalWatermark) {
			return createEventFromTimestamp(generateTimelyTimestamp(), globalWatermark, Timing.TIMELY);
		}

		@Override
		public long getLocalWatermark() {
			return orderedTimelyTimestamps.get(0);
		}

		@Override
		public boolean canProduceEventAtWatermark(long globalWatermark) {
			return true;
		}

		@Override
		public boolean hasMoreEvents() {
			return hasMoreTimelyEvents();
		}

		@Override
		public EventGenerator<K, E> getNextGenerator(long globalWatermark) {
			return new InLatenessGenerator();
		}
	}

	/**
	 * internal generator delegate for producing late session events with timestamps within the allowed lateness
	 */
	class InLatenessGenerator extends AbstractEventGenerator {

		@Override
		public E generateEvent(long globalWatermark) {
			return createEventFromTimestamp(generateLateTimestamp(), globalWatermark, Timing.IN_LATENESS);
		}

		@Override
		public long getLocalWatermark() {
			return getAfterLatenessTimestamp() - 1;
		}

		@Override
		public boolean canProduceEventAtWatermark(long globalWatermark) {
			return isTriggered(globalWatermark);
		}

		@Override
		public boolean hasMoreEvents() {
			return hasMoreInLatenessEvents();
		}

		@Override
		public EventGenerator<K, E> getNextGenerator(long globalWatermark) {
			return new AfterLatenessGenerator();
		}
	}

	/**
	 * internal generator delegate for producing late session events with timestamps after the lateness
	 */
	class AfterLatenessGenerator extends AbstractEventGenerator {

		@Override
		public E generateEvent(long globalWatermark) {
			return createEventFromTimestamp(generateLateTimestamp(), globalWatermark, Timing.AFTER_LATENESS);
		}

		@Override
		public long getLocalWatermark() {
			return getAfterLatenessTimestamp();
		}

		@Override
		public boolean canProduceEventAtWatermark(long globalWatermark) {
			return isAfterLateness(globalWatermark);
		}

		@Override
		public boolean hasMoreEvents() {
			return true;
		}

		@Override
		public EventGenerator<K, E> getNextGenerator(long globalWatermark) {
			throw new IllegalStateException("This generator has no successor");
		}
	}

}