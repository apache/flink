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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * {@link org.apache.flink.metrics.reporter.MetricReporter} that exports {@link org.apache.flink.metrics.Metric}s via JMX.
 *
 * Largely based on the JmxReporter class of the dropwizard metrics library
 * https://github.com/dropwizard/metrics/blob/master/metrics-core/src/main/java/io/dropwizard/metrics/JmxReporter.java
 */
@Internal
public class JMXReporter implements MetricReporter {
	private static final Logger LOG = LoggerFactory.getLogger(JMXReporter.class);

	private MBeanServer mBeanServer;

	private static final String PREFIX = "org.apache.flink.metrics:";
	private static final String KEY_PREFIX = "key";

	public JMXReporter() {
		this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String name) {
		AbstractBean jmxMetric;
		ObjectName jmxName;
		try {
			jmxName = new ObjectName(name);
		} catch (MalformedObjectNameException e) {
			throw new IllegalArgumentException("Metric name did not conform to JMX ObjectName rules: " + name, e);
		}

		if (metric instanceof Gauge) {
			jmxMetric = new JmxGauge((Gauge<?>) metric);
		} else if (metric instanceof Counter) {
			jmxMetric = new JmxCounter((Counter) metric);
		} else {
			throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());
		}

		try {
			mBeanServer.registerMBean(jmxMetric, jmxName);
		} catch (NotCompliantMBeanException e) { //implementation error on our side
			LOG.error("Metric did not comply with JMX MBean naming rules.", e);
		} catch (InstanceAlreadyExistsException e) {
			LOG.error("A metric with the name " + jmxName + " was already registered.", e);
		} catch (MBeanRegistrationException e) {
			LOG.error("Failed to register metric.", e);
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String name) {
		try {
			mBeanServer.unregisterMBean(new ObjectName(name));
		} catch (MBeanRegistrationException e) {
			LOG.error("Un-registering metric failed.", e);
		} catch (MalformedObjectNameException e) {
			LOG.error("Un-registering metric failed due to invalid name.", e);
		} catch (InstanceNotFoundException e) {
			//alright then
		}
	}

	@Override
	public void open(Configuration config) {
	}

	@Override
	public void close() {
	}

	@Override
	public String generateName(String name, List<String> origin) {
		StringBuilder fullName = new StringBuilder();

		fullName.append(PREFIX);
		for (int x = 0; x < origin.size(); x++) {
			fullName.append(KEY_PREFIX);
			fullName.append(x);
			fullName.append("=");
			String value = origin.get(x);
			value = value.replaceAll("\"", "");
			value = value.replaceAll(" ", "_");
			value = value.replaceAll("[,=;:?'*]", "-");
			fullName.append(value);
			fullName.append(",");
		}
		fullName.append("name=").append(name);

		return fullName.toString();
	}

	public interface MetricMBean {
	}

	private abstract static class AbstractBean implements MetricMBean {
	}

	public interface JmxCounterMBean extends MetricMBean {
		long getCount();
	}

	private static class JmxCounter extends AbstractBean implements JmxCounterMBean {
		private Counter counter;

		public JmxCounter(Counter counter) {
			this.counter = counter;
		}

		@Override
		public long getCount() {
			return counter.getCount();
		}
	}

	public interface JmxGaugeMBean extends MetricMBean {
		Object getValue();
	}

	private static class JmxGauge extends AbstractBean implements JmxGaugeMBean {
		
		private final Gauge<?> gauge;

		public JmxGauge(Gauge<?> gauge) {
			this.gauge = gauge;
		}

		@Override
		public Object getValue() {
			return gauge.getValue();
		}
	}
}
