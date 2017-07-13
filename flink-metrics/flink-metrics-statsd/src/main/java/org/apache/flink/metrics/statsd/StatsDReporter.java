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

package org.apache.flink.metrics.statsd;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporterV2;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Originally based on the StatsDReporter class by ReadyTalk.
 *
 * <p>https://github.com/ReadyTalk/metrics-statsd/blob/master/metrics3-statsd/src/main/java/com/readytalk/metrics/StatsDReporter.java
 *
 * <p>Ported since it was not present in maven central.
 */
@PublicEvolving
public class StatsDReporter extends AbstractReporterV2<StatsDReporter.TaggedMetric> implements Scheduled {

	class TaggedMetric {
		private final String name;
		private final String tags;
		TaggedMetric(String name, String tags) {
			this.name = name;
			this.tags = tags;
		}

		String getName() {
			return name;
		}

		String getTags() {
			return tags;
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(StatsDReporter.class);

	private static final String ARG_HOST = "host";
	private static final String ARG_PORT = "port";
	private static final String ARG_DOGSTATSD = "dogstatsd";
	private static final String ARG_SHORTIDS = "shortids";

	private boolean closed = false;

	private DatagramSocket socket;
	private InetSocketAddress address;

	private boolean dogstatsdMode;
	private boolean shortIds;

	private final Pattern instanceRef = Pattern.compile("@[a-f0-9]+");
	private final Pattern flinkId = Pattern.compile("[a-f0-9]{32}");
	private static final char SCOPE_SEPARATOR = '.';

	@Override
	public void open(MetricConfig config) {
		String host = config.getString(ARG_HOST, null);
		int port = config.getInteger(ARG_PORT, -1);

		dogstatsdMode = config.getBoolean(ARG_DOGSTATSD, false);
		shortIds = config.getBoolean(ARG_SHORTIDS, false);

		if (host == null || host.length() == 0 || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		this.address = new InetSocketAddress(host, port);

		try {
			this.socket = new DatagramSocket(0);
		} catch (SocketException e) {
			throw new RuntimeException("Could not create datagram socket. ", e);
		}
		log.info("Configured StatsDReporter with config: {}", config);
	}

	@Override
	public void close() {
		closed = true;
		if (socket != null && !socket.isClosed()) {
			socket.close();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Removes leading and trailing angle brackets.
	 */
	private String stripBrackets(String str) {
		return str.substring(1, str.length() - 1);
	}

	@SuppressWarnings("unchecked")
	private String getLogicalScope(MetricGroup group) {
		return ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(this, SCOPE_SEPARATOR);
	}

	@Override
	protected TaggedMetric getMetricInfo(Metric metric, String metricName, MetricGroup group) {
		if (dogstatsdMode) {
			String name = "flink" + SCOPE_SEPARATOR + getLogicalScope(group) + SCOPE_SEPARATOR + filterCharacters(metricName);

			// memoize dogstatsd tag section: "|#tag:val,tag:val,tag:val"
			StringBuilder statsdTagLine = new StringBuilder();
			for (Map.Entry<String, String> entry: group.getAllVariables().entrySet()) {
				String k = stripBrackets(entry.getKey());
				String v = filterCharacters(entry.getValue());
				if (statsdTagLine.length() == 0) {
					// begin tag section with dogstatsd format |#
					statsdTagLine.append("|#");
				} else {
					// separate multiple k:v with commas
					statsdTagLine.append(',');
				}
				statsdTagLine.append(k).append(':').append(v);
			}
			return new TaggedMetric(name, statsdTagLine.toString());
		} else {
			// the simple case with one scoped name
			String name = group.getMetricIdentifier(metricName, this);
			return new TaggedMetric(name, null);
		}
	}

	@Override
	public void report() {
		// instead of locking here, we tolerate exceptions
		// we do this to prevent holding the lock for very long and blocking
		// operator creation and shutdown
		try {
			for (Map.Entry<Gauge<?>, TaggedMetric> entry : gauges.entrySet()) {
				if (closed) {
					return;
				}
				reportGauge(entry.getValue(), entry.getKey());
			}

			for (Map.Entry<Counter, TaggedMetric> entry : counters.entrySet()) {
				if (closed) {
					return;
				}
				reportCounter(entry.getValue(), entry.getKey());
			}

			for (Map.Entry<Histogram, TaggedMetric> entry : histograms.entrySet()) {
				reportHistogram(entry.getValue(), entry.getKey());
			}

			for (Map.Entry<Meter, TaggedMetric> entry : meters.entrySet()) {
				reportMeter(entry.getValue(), entry.getKey());
			}
		}
		catch (ConcurrentModificationException | NoSuchElementException e) {
			// ignore - may happen when metrics are concurrently added or removed
			// report next time
		}
	}

	// ------------------------------------------------------------------------

	private void reportCounter(final TaggedMetric metric, final Counter counter) {
		send(metric.getName(), String.valueOf(counter.getCount()), metric.getTags());
	}

	private void reportGauge(final TaggedMetric metric, final Gauge<?> gauge) {
		Object value = gauge.getValue();
		if (value == null) {
			return;
		}
		if (value instanceof Map && gauge.getClass().getSimpleName().equals("LatencyGauge")) {
			// LatencyGauge is a Map<String, HashMap<String,Double>>
			for (Object m: ((Map<?, ?>) value).values()) {
				if (m instanceof Map) {
					for (Map.Entry<?, ?> entry: ((Map<?, ?>) m).entrySet()) {
						String k = String.valueOf(entry.getKey());
						String v = String.valueOf(entry.getValue());
						send(prefix(metric.getName(), k), v, metric.getTags());
					}
				}
			}
		} else {
			send(metric.getName(), value.toString(), metric.getTags());
		}
	}

	private void reportHistogram(final TaggedMetric metric, final Histogram histogram) {
		if (histogram != null) {

			HistogramStatistics statistics = histogram.getStatistics();

			if (statistics != null) {
				send(prefix(metric.getName(), "count"), String.valueOf(histogram.getCount()), metric.getTags());
				send(prefix(metric.getName(), "max"), String.valueOf(statistics.getMax()), metric.getTags());
				send(prefix(metric.getName(), "min"), String.valueOf(statistics.getMin()), metric.getTags());
				send(prefix(metric.getName(), "mean"), String.valueOf(statistics.getMean()), metric.getTags());
				send(prefix(metric.getName(), "stddev"), String.valueOf(statistics.getStdDev()), metric.getTags());
				send(prefix(metric.getName(), "p50"), String.valueOf(statistics.getQuantile(0.5)), metric.getTags());
				send(prefix(metric.getName(), "p75"), String.valueOf(statistics.getQuantile(0.75)), metric.getTags());
				send(prefix(metric.getName(), "p95"), String.valueOf(statistics.getQuantile(0.95)), metric.getTags());
				send(prefix(metric.getName(), "p98"), String.valueOf(statistics.getQuantile(0.98)), metric.getTags());
				send(prefix(metric.getName(), "p99"), String.valueOf(statistics.getQuantile(0.99)), metric.getTags());
				send(prefix(metric.getName(), "p999"), String.valueOf(statistics.getQuantile(0.999)), metric.getTags());
			}
		}
	}

	private void reportMeter(final TaggedMetric metric, final Meter meter) {
		if (meter != null) {
			send(prefix(metric.getName(), "rate"), String.valueOf(meter.getRate()), metric.getTags());
			send(prefix(metric.getName(), "count"), String.valueOf(meter.getCount()), metric.getTags());
		}
	}

	private String prefix(String ... names) {
		if (names.length > 0) {
			StringBuilder stringBuilder = new StringBuilder(names[0]);

			for (int i = 1; i < names.length; i++) {
				stringBuilder.append(SCOPE_SEPARATOR).append(names[i]);
			}

			return stringBuilder.toString();
		} else {
			return "";
		}
	}

	private String buildStatsdLine(final String name, final String value, final String tags) {
		Double number;
		try {
			number = Double.parseDouble(value);
		}
		catch (NumberFormatException e) {
			// quietly skip values like "n/a"
			return "";
		}
		if (number >= 0.) {
			return String.format("%s:%s|g%s", name, value, tags != null ? tags : "");
		} else {
			// quietly skip "unknowns" like lowWaterMark:-9223372036854775808, or JVM.Memory.NonHeap.Max:-1, or NaN
			return "";
		}
	}

	private void send(final String name, final String value, final String tags) {
		String formatted = buildStatsdLine(name, value, tags);
		if (formatted.length() > 0) {
			try {
				byte[] data = formatted.getBytes(StandardCharsets.UTF_8);
				socket.send(new DatagramPacket(data, data.length, this.address));
			}
			catch (IOException e) {
				LOG.error("unable to send packet to statsd at '{}:{}'", address.getHostName(), address.getPort());
			}
		}
	}

	/**
	* dogstatsd names should: start with letter, uses ascii alphanumerics and underscore, separated by periods.
	* Collapse runs of invalid characters into an underscore. Discard invalid prefix and suffix.
	* Eg: ":::metric:::name:::" ->  "metric_name"
	*/

	private boolean isValidStatsdChar(char c) {
		return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '_');
	}

	private String filterNCharacters(String input, int limit) {
		char[] chars = null;
		final int strLen = input.length();
		int pos = 0;
		boolean insertFiller = false;

		for (int i = 0; i < strLen && pos < limit; i++) {
			final char c = input.charAt(i);
			if (isValidStatsdChar(c)) {
				if (chars != null) {
					// skip invalid suffix, only fill if followed by valid character
					if (insertFiller) {
						chars[pos++] = '_';
						insertFiller = false;
					}
					chars[pos] = c;
				}
				pos++;
			} else {
				if (chars == null) {
					chars = input.toCharArray();
				}
				// skip invalid prefix, until pos > 0
				if (pos > 0) {
					// collapse sequence of invalid char into one filler
					insertFiller = true;
				}
			}
		}

		if (chars == null) {
			if (strLen > limit) {
				return input.substring(0, limit);
			} else {
				return input; // happy path, input is entirely valid and under the limit
			}
		} else {
			return new String(chars, 0, pos);
		}
	}

	/**
	 * filterCharacters() is called on each delimited segment of the metric.
	 *
	 * <p>We might get a string that has coded structures, references to instances of serializers and reducers, and even if
	 * we normalize all the odd characters could be overly long for a metric name, likely to be truncated downstream.
	 * Our choices here appear to be either to discard invalid metrics, or to pragmatically handle each of the various
	 * issues and produce something that might be useful in aggregate even though the named parts are hard to read.
	 *
	 * <p>This function will find and remove all object references like @abcd0123, so that task and operator names are stable.
	 * The name of an operator should be the same every time it is run, so we should ignore object hash ids like these.
	 *
	 * <p>If the segment is a tm_id, task_id, job_id, task_attempt_id, we can optionally trim those to the first 8 chars.
	 * This can reduce overall length substantially while still preserving enough to distinguish metrics from each other.
	 *
	 * <p>If the segment is 50 chars or longer, we will compress it to avoid truncation. The compression will look like the
	 * first 10 valid chars followed by a hash of the original.  This sacrifices readability for utility as a metric, so
	 * that latency metrics might survive with valid and useful dimensions for aggregation, even if it is very hard to
	 * reverse engineer the particular operator name.  Application developers can of course still supply their own names
	 * and are not forced to rely on the defaults.
	 *
	 * <p>This will turn something like:
	 * 		"TriggerWindow(TumblingProcessingTimeWindows(5000), ReducingStateDescriptor{serializer=org.apache.flink.api.java
	 * 		.typeutils.runtime.PojoSerializer@f3395ffa, reduceFunction=org.apache.flink.streaming.examples.socket.
	 * 		SocketWindowWordCount$1@4201c465}, ProcessingTimeTrigger(), WindowedStream.reduce(WindowedStream.java-301))"
	 *
	 * <p>into:  "TriggerWin_c2910b88"
	 */
	@Override
	public String filterCharacters(String input) {
		// remove instance references
		Matcher hasRefs = instanceRef.matcher(input);
		if (hasRefs.find()) {
			input = hasRefs.replaceAll("");
		}
		// compress segments too long
		if (input.length() >= 50) {
			return filterNCharacters(input, 10) + "_" + Integer.toHexString(input.hashCode());
		}
		int limit = Integer.MAX_VALUE;
		// optionally shrink flink ids
		if (shortIds && input.length() == 32 && flinkId.matcher(input).matches()) {
			limit = 8;
		}
		return filterNCharacters(input, limit);
	}
}
