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
import org.apache.flink.metrics.groups.AbstractMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via JMX.
 *
 * Largely based on the JmxReporter class of the dropwizard metrics library
 * https://github.com/dropwizard/metrics/blob/master/metrics-core/src/main/java/io/dropwizard/metrics/JmxReporter.java
 */
@Internal
public class JMXReporter implements MetricReporter {

	private static final String PREFIX = "org.apache.flink.metrics:";
	private static final String KEY_PREFIX = "key";

	private static final Logger LOG = LoggerFactory.getLogger(JMXReporter.class);

	// ------------------------------------------------------------------------

	/** The server where the management beans are registered and deregistered */
	private final MBeanServer mBeanServer;

	/** The names under which the registered metrics have been added to the MBeanServer */ 
	private final Map<Metric, ObjectName> registeredMetrics;

	/**
	 * Creates a new JMXReporter
	 */
	public JMXReporter() {
		this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
		this.registeredMetrics = new HashMap<>();
	}

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration config) {}

	@Override
	public void close() {}

	// ------------------------------------------------------------------------
	//  adding / removing metrics
	// ------------------------------------------------------------------------

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, AbstractMetricGroup group) {
		final String name = generateJmxName(metricName, group.getScopeComponents());

		AbstractBean jmxMetric;
		ObjectName jmxName;
		try {
			jmxName = new ObjectName(name);
		} catch (MalformedObjectNameException e) {
			LOG.error("Metric name did not conform to JMX ObjectName rules: " + name, e);
			return;
		}

		if (metric instanceof Gauge) {
			jmxMetric = new JmxGauge((Gauge<?>) metric);
		} else if (metric instanceof Counter) {
			jmxMetric = new JmxCounter((Counter) metric);
		} else {
			LOG.error("Unknown metric type: " + metric.getClass().getName());
			return;
		}

		try {
			synchronized (this) {
				mBeanServer.registerMBean(jmxMetric, jmxName);
				registeredMetrics.put(metric, jmxName);
			}
		} catch (NotCompliantMBeanException e) {
			// implementation error on our side
			LOG.error("Metric did not comply with JMX MBean naming rules.", e);
		} catch (InstanceAlreadyExistsException e) {
			LOG.error("A metric with the name " + jmxName + " was already registered.", e);
		} catch (Throwable t) {
			LOG.error("Failed to register metric", t);
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, AbstractMetricGroup group) {
		try {
			synchronized (this) {
				final ObjectName jmxName = registeredMetrics.remove(metric);

				// remove the metric if it is known. if it is not known, ignore the request
				if (jmxName != null) {
					mBeanServer.unregisterMBean(jmxName);
				}
			}
		} catch (InstanceNotFoundException e) {
			// alright then
		} catch (Throwable t) {
			// never propagate exceptions - the metrics reporter should not affect the stability
			// of the running system
			LOG.error("Un-registering metric failed", t);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities 
	// ------------------------------------------------------------------------

	static String generateJmxName(String metricName, String[] scopeComponents) {
		final StringBuilder nameBuilder = new StringBuilder(128);
		nameBuilder.append(PREFIX);

		for (int x = 0; x < scopeComponents.length; x++) {
			// write keyX=
			nameBuilder.append(KEY_PREFIX);
			nameBuilder.append(x);
			nameBuilder.append("=");

			// write scopeName component
			nameBuilder.append(replaceInvalidChars(scopeComponents[x]));
			nameBuilder.append(",");
		}

		// write the name
		nameBuilder.append("name=").append(replaceInvalidChars(metricName));

		return nameBuilder.toString();
	}
	
	/**
	 * Lightweight method to replace unsupported characters.
	 * If the string does not contain any unsupported characters, this method creates no
	 * new string (and in fact no new objects at all).
	 * 
	 * <p>Replacements:
	 * 
	 * <ul>
	 *     <li>{@code "} is removed</li>
	 *     <li>{@code space} is replaced by {@code _} (underscore)</li>
	 *     <li>{@code , = ; : ? ' *} are replaced by {@code -} (hyphen)</li>
	 * </ul>
	 */
	static String replaceInvalidChars(String str) {
		char[] chars = null;
		final int strLen = str.length();
		int pos = 0;
		
		for (int i = 0; i < strLen; i++) {
			final char c = str.charAt(i);
			switch (c) {
				case '"':
					// remove character by not moving cursor
					if (chars == null) {
						chars = str.toCharArray();
					}
					break;

				case ' ':
					if (chars == null) {
						chars = str.toCharArray();
					}
					chars[pos++] = '_';
					break;
				
				case ',':
				case '=':
				case ';':
				case ':':
				case '?':
				case '\'':
				case '*':
					if (chars == null) {
						chars = str.toCharArray();
					}
					chars[pos++] = '-';
					break;

				default:
					if (chars != null) {
						chars[pos] = c;
					}
					pos++;
			}
		}
		
		return chars == null ? str : new String(chars, 0, pos);
	}

	// ------------------------------------------------------------------------
	//  Interfaces and base classes for JMX beans 
	// ------------------------------------------------------------------------

	public interface MetricMBean {}

	private abstract static class AbstractBean implements MetricMBean {}

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
