/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.metrics.reporter;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Timer;

import java.util.Map;

/**
 * Marker interface for reporters that actively send out data periodically.
 */
@PublicEvolving
public interface Scheduled {
	/**
	 * Report the current measurements.
	 * This method is called in regular intervals
	 *
	 * @param gauges     registered gauges
	 * @param counters   registered counters
	 * @param histograms registered histograms
	 * @param meters     registered meters
	 * @param timers     registered timers
	 */
	void report(Map<String, Gauge> gauges,
				Map<String, Counter> counters,
				Map<String, Histogram> histograms,
				Map<String, Meter> meters,
				Map<String, Timer> timers);
}
