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
package org.apache.flink.metrics.graphite;

import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.ScheduledDropwizardReporter;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class GraphiteReporter extends ScheduledDropwizardReporter {
	@Override
	public ScheduledReporter getReporter(Configuration config) {
		String host = config.getString(ARG_HOST, null);
		int port = config.getInteger(ARG_PORT, -1);

		if (host == null || host.length() == 0 || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		String prefix = config.getString(ARG_PREFIX, null);
		String conversionRate = config.getString(ARG_CONVERSION_RATE, null);
		String conversionDuration = config.getString(ARG_CONVERSION_DURATION, null);

		com.codahale.metrics.graphite.GraphiteReporter.Builder builder =
			com.codahale.metrics.graphite.GraphiteReporter.forRegistry(registry);

		if (prefix != null) {
			builder.prefixedWith(prefix);
		}

		if (conversionRate != null) {
			builder.convertRatesTo(TimeUnit.valueOf(conversionRate));
		}

		if (conversionDuration != null) {
			builder.convertDurationsTo(TimeUnit.valueOf(conversionDuration));
		}

		return builder.build(new Graphite(host, port));
	}

	@Override
	public String generateName(String name, List<String> scope) {
		StringBuilder sb = new StringBuilder();
		for (String s : scope) {
			sb.append(s.replace(".", "_").replace("\"", ""));
			sb.append(".");
		}
		sb.append(name);
		return sb.toString();
	}
}
