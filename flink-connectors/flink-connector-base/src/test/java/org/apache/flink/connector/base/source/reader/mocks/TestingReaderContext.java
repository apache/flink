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

package org.apache.flink.connector.base.source.reader.mocks;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * A testing implementation of the {@link SourceReaderContext}.
 */
public class TestingReaderContext implements SourceReaderContext {

	private final UnregisteredMetricsGroup metrics = new UnregisteredMetricsGroup();

	private final Configuration config;

	private final ArrayList<SourceEvent> sentEvents = new ArrayList<>();

	public TestingReaderContext() {
		this(new Configuration());
	}

	public TestingReaderContext(Configuration config) {
		this.config = config;
	}

	// ------------------------------------------------------------------------

	@Override
	public MetricGroup metricGroup() {
		return metrics;
	}

	@Override
	public Configuration getConfiguration() {
		return config;
	}

	@Override
	public String getLocalHostName() {
		return "localhost";
	}

	@Override
	public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
		sentEvents.add(sourceEvent);
	}

	// ------------------------------------------------------------------------

	public List<SourceEvent> getSentEvents() {
		return new ArrayList<>(sentEvents);
	}

	public void clearSentEvents() {
		sentEvents.clear();
	}
}
