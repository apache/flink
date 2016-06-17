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
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.metrics.MetricRegistry.KEY_METRICS_JMX_PORT;

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

	/** The server to which JMX clients connect to. ALlows for better control over port usage. */
	private JMXServer jmxServer;

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
	public void open(Configuration config) {
		this.jmxServer = startJmxServer(config);
	}

	private static JMXServer startJmxServer(Configuration config) {
		JMXServer jmxServer;

		String portRange = config.getString(KEY_METRICS_JMX_PORT, "9010-9025");
		String[] ports = portRange.split("-");

		if (ports.length == 0 || ports.length > 2) {
			throw new IllegalArgumentException("JMX port range was configured incorrectly. " +
				"Expected: <startPort>[-<endPort>] Configured: " + portRange);
		}

		if (ports.length == 1) { //single port was configured
			int port = Integer.parseInt(ports[0]);
			jmxServer = new JMXServer(port);
			try {
				jmxServer.start();
			} catch (IOException e) {
				throw new RuntimeException("Could not start JMX server on port " + port + ".");
			}
			return jmxServer;
		} else { //port range was configured
			int start = Integer.parseInt(ports[0]);
			int end = Integer.parseInt(ports[1]);
			while (true) {
				try {
					jmxServer = new JMXServer(start);
					jmxServer.start();
					LOG.info("Starting JMX on port " + start + ".");
					break;
				} catch (IOException e) { //assume port conflict
					LOG.debug("Could not start JMX server. Attempting different port", e);
					start++;
					if (start > end) {
						throw new RuntimeException("Could not start JMX server.", e);
					}
				}
			}
		}
		return jmxServer;
	}

	@Override
	public void close() {
		if (jmxServer != null) {
			try {
				jmxServer.stop();
			} catch (IOException e) {
				LOG.error("Failed to stop JMX server.", e);
			}
		}
	}

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

	/**
	 * JMX Server implementation that JMX clients can connect to.
	 *
	 * Heavily based on j256 simplejmx project
	 *
	 * https://github.com/j256/simplejmx/blob/master/src/main/java/com/j256/simplejmx/server/JmxServer.java
	 */
	private static class JMXServer {
		private int port;
		private Registry rmiRegistry;
		private JMXConnectorServer connector;

		public JMXServer(int port) {
			this.port = port;
		}

		public void start() throws IOException {
			startRmiRegistry();
			startJmxService();
		}

		public void stop() throws IOException {
			if (connector != null) {
				try {
					connector.stop();
				} finally {
					connector = null;
				}
			}
			if (rmiRegistry != null) {
				try {
					UnicastRemoteObject.unexportObject(rmiRegistry, true);
				} catch (NoSuchObjectException e) {
					throw new IOException("Could not unexport our RMI registry", e);
				} finally {
					rmiRegistry = null;
				}
			}
		}

		private void startRmiRegistry() throws IOException {
			if (rmiRegistry != null) {
				return;
			}
			rmiRegistry = LocateRegistry.createRegistry(port);
		}

		private void startJmxService() throws IOException {
			if (connector != null) {
				return;
			}
			String serverHost = "localhost";
			String registryHost = "";
			String serviceUrl =
				"service:jmx:rmi://" + serverHost + ":" + port + "/jndi/rmi://" + registryHost + ":" + port + "/jmxrmi";
			JMXServiceURL url;
			try {
				url = new JMXServiceURL(serviceUrl);
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("Malformed service url created " + serviceUrl, e);
			}

			connector = JMXConnectorServerFactory.newJMXConnectorServer(url, null, ManagementFactory.getPlatformMBeanServer());

			connector.start();
		}
	}
}
