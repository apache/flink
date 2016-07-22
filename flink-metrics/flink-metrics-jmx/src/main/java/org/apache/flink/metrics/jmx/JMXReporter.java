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

package org.apache.flink.metrics.jmx;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;

import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.util.NetUtils;
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
import java.util.Iterator;
import java.util.Map;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via JMX.
 *
 * Largely based on the JmxReporter class of the dropwizard metrics library
 * https://github.com/dropwizard/metrics/blob/master/metrics-core/src/main/java/io/dropwizard/metrics/JmxReporter.java
 */
public class JMXReporter implements MetricReporter {

	private static final String PREFIX = "org.apache.flink.metrics:";
	private static final String KEY_PREFIX = "key";

	public static final String ARG_PORT = "port";

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
	public void open(MetricConfig config) {
		String portsConfig = config.getString(ARG_PORT, null);

		if (portsConfig != null) {
			Iterator<Integer> ports = NetUtils.getPortRangeFromString(portsConfig);

			JMXServer server = new JMXServer();
			while (ports.hasNext()) {
				int port = ports.next();
				try {
					server.start(port);
					LOG.info("Started JMX server on port " + port + ".");
					// only set our field if the server was actually started
					jmxServer = server;
					break;
				} catch (IOException ioe) { //assume port conflict
					LOG.debug("Could not start JMX server on port " + port + ".", ioe);
					try {
						server.stop();
					} catch (Exception e) {
						LOG.debug("Could not stop JMX server.", e);
					}
				}
			}
			if (jmxServer == null) {
				throw new RuntimeException("Could not start JMX server on any configured port. Ports: " + portsConfig);
			}
		}
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
	
	public int getPort() {
		if (jmxServer == null) {
			throw new NullPointerException("No server was opened. Did you specify a port?");
		}
		return jmxServer.port;
	}

	// ------------------------------------------------------------------------
	//  adding / removing metrics
	// ------------------------------------------------------------------------

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
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
		} else if (metric instanceof Histogram) {
			jmxMetric = new JmxHistogram((Histogram) metric);
		} else {
			LOG.error("Cannot add unknown metric type: {}. This indicates that the metric type " +
				"is not supported by this reporter.", metric.getClass().getName());
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
			LOG.debug("A metric with the name " + jmxName + " was already registered.", e);
			LOG.error("A metric with the name " + jmxName + " was already registered.");
		} catch (Throwable t) {
			LOG.error("Failed to register metric", t);
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
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

			// write scope component
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

		JmxCounter(Counter counter) {
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

		JmxGauge(Gauge<?> gauge) {
			this.gauge = gauge;
		}

		@Override
		public Object getValue() {
			return gauge.getValue();
		}
	}

	public interface JmxHistogramMBean extends MetricMBean {
		long getCount();

		double getMean();

		double getStdDev();

		long getMax();

		long getMin();

		double getMedian();

		double get75thPercentile();

		double get95thPercentile();

		double get98thPercentile();

		double get99thPercentile();

		double get999thPercentile();
	}

	private static class JmxHistogram extends AbstractBean implements JmxHistogramMBean {

		private final Histogram histogram;

		JmxHistogram(Histogram histogram) {
			this.histogram = histogram;
		}

		@Override
		public long getCount() {
			return histogram.getCount();
		}

		@Override
		public double getMean() {
			return histogram.getStatistics().getMean();
		}

		@Override
		public double getStdDev() {
			return histogram.getStatistics().getStdDev();
		}

		@Override
		public long getMax() {
			return histogram.getStatistics().getMax();
		}

		@Override
		public long getMin() {
			return histogram.getStatistics().getMin();
		}

		@Override
		public double getMedian() {
			return histogram.getStatistics().getQuantile(0.5);
		}

		@Override
		public double get75thPercentile() {
			return histogram.getStatistics().getQuantile(0.75);
		}

		@Override
		public double get95thPercentile() {
			return histogram.getStatistics().getQuantile(0.95);
		}

		@Override
		public double get98thPercentile() {
			return histogram.getStatistics().getQuantile(0.98);
		}

		@Override
		public double get99thPercentile() {
			return histogram.getStatistics().getQuantile(0.99);
		}

		@Override
		public double get999thPercentile() {
			return histogram.getStatistics().getQuantile(0.999);
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
		private Registry rmiRegistry;
		private JMXConnectorServer connector;
		private int port;

		public void start(int port) throws IOException {
			if (rmiRegistry != null && connector != null) {
				LOG.debug("JMXServer is already running.");
				return;
			}
			startRmiRegistry(port);
			startJmxService(port);
			this.port = port;
		}

		/**
		 * Starts an RMI Registry that allows clients to lookup the JMX IP/port.
		 *
		 * @param port rmi port to use
		 * @throws IOException
		 */
		private void startRmiRegistry(int port) throws IOException {
			rmiRegistry = LocateRegistry.createRegistry(port);
		}

		/**
		 * Starts a JMX connector that allows (un)registering MBeans with the MBean server and RMI invocations.
		 *
		 * @param port jmx port to use
		 * @throws IOException
		 */
		private void startJmxService(int port) throws IOException {
			String serviceUrl = "service:jmx:rmi://localhost:" + port + "/jndi/rmi://localhost:" + port + "/jmxrmi";
			JMXServiceURL url;
			try {
				url = new JMXServiceURL(serviceUrl);
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("Malformed service url created " + serviceUrl, e);
			}

			connector = JMXConnectorServerFactory.newJMXConnectorServer(url, null, ManagementFactory.getPlatformMBeanServer());

			connector.start();
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
					throw new IOException("Could not un-export our RMI registry", e);
				} finally {
					rmiRegistry = null;
				}
			}
		}
	}
}
