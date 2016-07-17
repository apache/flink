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

import java.util.HashMap;
import java.util.Map;


/**
 * Produces the session event generators
 *
 * @param <K>
 */
public class EventGeneratorFactory<K, E> {

	// map key -> latest generator for this key
	private final Map<K, EventGenerator<K, E>> latestGeneratorsByKey;

	// pseudo random engine
	private final LongRandomGenerator randomGenerator;

	// configuration for the streams that are simulated by the generators this factory creates
	private final StreamConfiguration streamConfiguration;

	// factory for the events that is employed be the event generators this factory creates
	private final StreamEventFactory<K, E> eventFactory;

	// number of timely events generators produce per session
	private final int timelyEventsPerSession;

	// the max. gap between events that belong to one session
	private final long maxSessionEventGap;

	// counter that tracks how many generators this has produced
	private int producedGeneratorsCount;

	/**
	 * @param streamConfiguration
	 * @param eventFactory
	 * @param sessionTimeout
	 * @param timelyEventsPerSession
	 * @param randomGenerator
	 */
	public EventGeneratorFactory(
			StreamConfiguration streamConfiguration,
			StreamEventFactory<K, E> eventFactory,
			long sessionTimeout,
			int timelyEventsPerSession,
			LongRandomGenerator randomGenerator) {

		Preconditions.checkNotNull(streamConfiguration);
		Preconditions.checkNotNull(randomGenerator);
		Preconditions.checkArgument(sessionTimeout >= 0);
		Preconditions.checkArgument(timelyEventsPerSession >= 0);

		this.latestGeneratorsByKey = new HashMap<>();
		this.streamConfiguration = streamConfiguration;
		this.eventFactory = eventFactory;
		this.randomGenerator = randomGenerator;
		this.maxSessionEventGap = sessionTimeout;
		this.timelyEventsPerSession = timelyEventsPerSession;
		this.producedGeneratorsCount = 0;
	}

	/**
	 * @param key
	 * @param globalWatermark
	 * @return
	 */
	public EventGenerator<K, E> newSessionStreamForKey(K key, long globalWatermark) {
		EventGenerator<K, E> eventGenerator = latestGeneratorsByKey.get(key);

		if (eventGenerator == null) {
			SessionConfiguration<K, E> sessionConfiguration = SessionConfiguration.of(
					key,
					0,
					maxSessionEventGap,
					globalWatermark,
					timelyEventsPerSession,
					eventFactory);
			SessionStreamConfiguration<K, E> sessionStreamConfiguration =
					new SessionStreamConfiguration<>(sessionConfiguration, streamConfiguration);
			eventGenerator = new SessionEventGeneratorImpl<>(sessionStreamConfiguration, randomGenerator);
		} else {
			eventGenerator = eventGenerator.getNextGenerator(globalWatermark);
		}
		latestGeneratorsByKey.put(key, eventGenerator);
		++producedGeneratorsCount;
		return eventGenerator;
	}

	/**
	 * @return
	 */
	public StreamConfiguration getStreamConfiguration() {
		return streamConfiguration;
	}

	/**
	 * @return
	 */
	public long getMaxSessionEventGap() {
		return maxSessionEventGap;
	}

	/**
	 * @return
	 */
	public int getProducedGeneratorsCount() {
		return producedGeneratorsCount;
	}

	static class PreviousSessionMetaData {
		int sessionId;
		long maxTimestamp;
	}
}