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

package org.apache.flink.metrics.signalfx;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import com.signalfx.endpoint.SignalFxEndpoint;
import com.signalfx.endpoint.SignalFxReceiverEndpoint;
import com.signalfx.metrics.auth.StaticAuthToken;
import com.signalfx.metrics.connection.HttpDataPointProtobufReceiverFactory;
import com.signalfx.metrics.connection.HttpEventProtobufReceiverFactory;
import com.signalfx.metrics.errorhandler.MetricError;
import com.signalfx.metrics.errorhandler.OnSendErrorHandler;
import com.signalfx.metrics.flush.AggregateMetricSender;
import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Base class for {@link org.apache.flink.metrics.reporter.MetricReporter} that wraps a
 * SignalFX {@link org.apache.flink.metrics.signalfx.SignalFXReporter}.
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.signalfx.SignalFXReporterFactory")
public class SignalFXReporter implements MetricReporter, Scheduled {
	protected final Logger log = LoggerFactory.getLogger(getClass());
	private String token;
	private String filters;
	private AggregateMetricSender reporter;

	private final Map<Gauge<?>, SGauge> gauges = new HashMap<>();
	private final Map<Counter, SCounter> counters = new HashMap<>();
	private final Map<Meter, SMeter> meters = new HashMap<>();
	// ------------------------------------------------------------------------

	public SignalFXReporter(@Nullable String token, @Nullable String filters) {
		this.token = token;
		this.filters = filters;
	}

	// ------------------------------------------------------------------------
	//  Getters
	// ------------------------------------------------------------------------
	@VisibleForTesting
	Map<Counter, SCounter> getCounters() {
		return counters;
	}

	@VisibleForTesting
	Map<Meter, SMeter> getMeters() {
		return meters;
	}

	@VisibleForTesting
	Map<Gauge<?>, SGauge> getGauges() {
		return gauges;
	}

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------
	@Override
	public void open(MetricConfig config) {
		this.reporter = getReporter();
	}

	@Override
	public void close() {
	}

	// ------------------------------------------------------------------------
	//  adding / removing metrics
	// ------------------------------------------------------------------------

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		// generate SFX dimensions from MetricGroup
		ArrayList<SignalFxProtocolBuffers.Dimension> dimensions = new ArrayList<>();
		ArrayList<String> components = new ArrayList(Arrays.asList(group.getScopeComponents()));

		for (Map.Entry<String, String> e : group.getAllVariables().entrySet()) {
			if (components.contains(e.getValue())) {
				String k = e.getKey().substring(1, e.getKey().length() - 1);
				dimensions.add(SignalFxProtocolBuffers.Dimension.newBuilder()
					.setKey(k)
					.setValue(e.getValue())
					.build());
				components.remove(e.getValue());
			}
		}
		// combine the final new metric name
		components.add(metricName);
		String name = String.join(".", components);

		for (String filter : filters.split(",")) {
			if (!filter.equals("") && name.contains(filter)) {
				log.info(String.format("filter out metric: %s", name));
				return;
			}
		}

		synchronized (this) {
			if (metric instanceof Counter) {
				counters.put((Counter) metric, new SCounter((Counter) metric, name, dimensions));
			} else if (metric instanceof Gauge) {
				if (((Gauge) metric).getValue() instanceof Number) {
					gauges.put((Gauge) metric, new SGauge((Gauge) metric, name, dimensions));
				}
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, new SMeter((Meter) metric, name, dimensions));
			} else if (metric instanceof Histogram) {
				log.warn(String.format("SignalFXReporter does not support Histogram: %s", name));
			} else {
				log.warn("Cannot add metric of type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		synchronized (this) {
			SMetric sMetric;

			if (metric instanceof Counter) {
				sMetric = counters.remove(metric);
			} else if (metric instanceof Gauge) {
				sMetric = gauges.remove(metric);
			} else if (metric instanceof Meter) {
				sMetric = meters.remove(metric);
			} else {
				sMetric = null;
			}
			if (sMetric != null) {
				log.info(String.format("metric %s has been removed", sMetric.metricName));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  scheduled reporting
	// ------------------------------------------------------------------------

	@Override
	public void report() {
		try (AggregateMetricSender.Session i = reporter.createSession()) {
			for (SGauge gauge : gauges.values()) {
				i.setDatapoint(gauge.getDataPoint());
			}
			for (SCounter counter : counters.values()) {
				i.setDatapoint(counter.getDataPoint());
			}
			for (SMeter meter : meters.values()) {
				i.setDatapoint(meter.getDataPoint());
			}
		} catch (IOException e) {
			log.warn("SignalFX reporter session error", e);
		}
	}

	public AggregateMetricSender getReporter() {
		// ------------------------------------------------------------------------
		SignalFxReceiverEndpoint signalFxEndpoint = new SignalFxEndpoint();
		return new AggregateMetricSender("flink",
			new HttpDataPointProtobufReceiverFactory(signalFxEndpoint).setVersion(2),
			new HttpEventProtobufReceiverFactory(signalFxEndpoint),
			new StaticAuthToken(this.token),
			Collections.<OnSendErrorHandler>singleton(new OnSendErrorHandler() {
				@Override
				public void handleError(MetricError e) {
					log.warn("Unable to POST metrics", e);
				}
			}));
	}
}
