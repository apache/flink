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
 * Produces the session event generators.
 *
 * @param <K> type of session keys
 */
public class EventGeneratorFactory<K, E> {

	// map key -> latest generator for this key
	private final Map<K, EventGenerator<K, E>> latestGeneratorsByKey;

	// pseudo random engine
	private final LongRandomGenerator randomGenerator;

	// configuration for the streams that are simulated by the generators this factory creates
	private final GeneratorConfiguration generatorConfiguration;

	// factory for the events that is employed be the event generators this factory creates
	private final GeneratorEventFactory<K, E> eventFactory;

	// number of timely events generators produce per session
	private final int timelyEventsPerSession;

	// the max. gap between events that belong to one session
	private final long maxSessionEventGap;

	// counter that tracks how many generators this has produced
	private int producedGeneratorsCount;

	public EventGeneratorFactory(
			GeneratorConfiguration generatorConfiguration,
			GeneratorEventFactory<K, E> eventFactory,
			long sessionTimeout,
			int timelyEventsPerSession,
			LongRandomGenerator randomGenerator) {

		Preconditions.checkNotNull(generatorConfiguration);
		Preconditions.checkNotNull(randomGenerator);
		Preconditions.checkArgument(sessionTimeout >= 0);
		Preconditions.checkArgument(timelyEventsPerSession >= 0);

		this.latestGeneratorsByKey = new HashMap<>();
		this.generatorConfiguration = generatorConfiguration;
		this.eventFactory = eventFactory;
		this.randomGenerator = randomGenerator;
		this.maxSessionEventGap = sessionTimeout;
		this.timelyEventsPerSession = timelyEventsPerSession;
		this.producedGeneratorsCount = 0;
	}

	/**
	 * @param key the key for the new session generator to instantiate
	 * @param globalWatermark the current global watermark
	 * @return a new event generator instance that generates events for the provided session key
	 */
	public EventGenerator<K, E> newSessionGeneratorForKey(K key, long globalWatermark) {
		EventGenerator<K, E> eventGenerator = latestGeneratorsByKey.get(key);

		if (eventGenerator == null) {
			SessionConfiguration<K, E> sessionConfiguration = SessionConfiguration.of(
					key,
					0,
					maxSessionEventGap,
					globalWatermark,
					timelyEventsPerSession,
					eventFactory);
			SessionGeneratorConfiguration<K, E> sessionGeneratorConfiguration =
					new SessionGeneratorConfiguration<>(sessionConfiguration, generatorConfiguration);
			eventGenerator = new SessionEventGeneratorImpl<>(sessionGeneratorConfiguration, randomGenerator);
		} else {
			eventGenerator = eventGenerator.getNextGenerator(globalWatermark);
		}
		latestGeneratorsByKey.put(key, eventGenerator);
		++producedGeneratorsCount;
		return eventGenerator;
	}

	public int getProducedGeneratorsCount() {
		return producedGeneratorsCount;
	}
}
