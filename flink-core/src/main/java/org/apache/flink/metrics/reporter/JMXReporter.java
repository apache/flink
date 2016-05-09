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
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.impl.CounterMetric;
import org.apache.flink.metrics.impl.GaugeMetric;
import org.apache.flink.metrics.impl.HistogramMetric;
import org.apache.flink.metrics.impl.MeterMetric;
import org.apache.flink.metrics.impl.TimerMetric;
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
import java.util.concurrent.TimeUnit;

/**
 * {@link org.apache.flink.metrics.reporter.MetricReporter} that exports {@link org.apache.flink.metrics.Metric}s via JMX.
 *
 * Largely based on the JmxReporter class of the dropwizard metrics library
 * https://github.com/dropwizard/metrics/blob/master/metrics-core/src/main/java/io/dropwizard/metrics/JmxReporter.java
 */
@Internal
public class JMXReporter implements MetricReporter, Listener {
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

		if (metric instanceof GaugeMetric) {
			jmxMetric = new JmxGauge((GaugeMetric<?>) metric);
		} else if (metric instanceof CounterMetric) {
			jmxMetric = new JmxCounter((CounterMetric) metric);
		} else if (metric instanceof HistogramMetric) {
			jmxMetric = new JmxHistogram((HistogramMetric) metric);
		} else if (metric instanceof MeterMetric) {
			jmxMetric = new JmxMeter((MeterMetric) metric, TimeUnit.SECONDS);
		} else if (metric instanceof TimerMetric) {
			jmxMetric = new JmxTimer((TimerMetric) metric, TimeUnit.SECONDS, TimeUnit.MICROSECONDS);
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
		fullName.append("name=" + name);

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
		private CounterMetric counter;

		public JmxCounter(CounterMetric counter) {
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
		private final GaugeMetric gauge;

		public JmxGauge(GaugeMetric gauge) {
			this.gauge = gauge;
		}

		@Override
		public Object getValue() {
			return gauge.getValue();
		}
	}

	public interface JmxHistogramMBean extends MetricMBean {
		long getCount();

		long getMin();

		long getMax();

		double getMean();

		double getStdDev();

		double get50thPercentile();

		double get75thPercentile();

		double get95thPercentile();

		double get98thPercentile();

		double get99thPercentile();

		double get999thPercentile();

		long[] values();
	}

	private static class JmxHistogram extends AbstractBean implements JmxHistogramMBean {
		private final HistogramMetric metric;

		private JmxHistogram(HistogramMetric metric) {
			this.metric = metric;
		}

		@Override
		public double get50thPercentile() {
			return metric.getSnapshot().getMedian();
		}

		@Override
		public long getCount() {
			return metric.getCount();
		}

		@Override
		public long getMin() {
			return metric.getSnapshot().getMin();
		}

		@Override
		public long getMax() {
			return metric.getSnapshot().getMax();
		}

		@Override
		public double getMean() {
			return metric.getSnapshot().getMean();
		}

		@Override
		public double getStdDev() {
			return metric.getSnapshot().getStdDev();
		}

		@Override
		public double get75thPercentile() {
			return metric.getSnapshot().get75thPercentile();
		}

		@Override
		public double get95thPercentile() {
			return metric.getSnapshot().get95thPercentile();
		}

		@Override
		public double get98thPercentile() {
			return metric.getSnapshot().get98thPercentile();
		}

		@Override
		public double get99thPercentile() {
			return metric.getSnapshot().get99thPercentile();
		}

		@Override
		public double get999thPercentile() {
			return metric.getSnapshot().get999thPercentile();
		}

		@Override
		public long[] values() {
			return metric.getSnapshot().getValues();
		}
	}

	public interface JmxMeterMBean extends MetricMBean {
		long getCount();

		double getMeanRate();

		double getOneMinuteRate();

		double getFiveMinuteRate();

		double getFifteenMinuteRate();

		String getRateUnit();
	}

	private static class JmxMeter extends AbstractBean implements JmxMeterMBean {
		private final MeterMetric meter;
		private final long rateFactor;
		private final String rateUnit;

		public JmxMeter(MeterMetric meter, TimeUnit rateUnit) {
			this.meter = meter;
			this.rateFactor = rateUnit.toSeconds(1);
			this.rateUnit = "events/" + calculateRateUnit(rateUnit);
		}

		@Override
		public long getCount() {
			return meter.getCount();
		}

		@Override
		public double getMeanRate() {
			return meter.getMeanRate() * rateFactor;
		}

		@Override
		public double getOneMinuteRate() {
			return meter.getOneMinuteRate() * rateFactor;
		}

		@Override
		public double getFiveMinuteRate() {
			return meter.getFiveMinuteRate() * rateFactor;
		}

		@Override
		public double getFifteenMinuteRate() {
			return meter.getFifteenMinuteRate() * rateFactor;
		}

		@Override
		public String getRateUnit() {
			return rateUnit;
		}

		private String calculateRateUnit(TimeUnit unit) {
			final String s = unit.toString().toLowerCase();
			return s.substring(0, s.length() - 1);
		}
	}

	public interface JmxTimerMBean extends JmxMeterMBean {
		double getMin();

		double getMax();

		double getMean();

		double getStdDev();

		double get50thPercentile();

		double get75thPercentile();

		double get95thPercentile();

		double get98thPercentile();

		double get99thPercentile();

		double get999thPercentile();

		long[] values();

		String getDurationUnit();
	}

	private static class JmxTimer extends AbstractBean implements JmxTimerMBean {
		private final TimerMetric metric;
		private final double durationFactor;
		private final String durationUnit;
		private final long rateFactor;
		private final String rateUnit;

		private JmxTimer(TimerMetric metric, TimeUnit rateUnit, TimeUnit durationUnit) {
			this.metric = metric;
			this.durationFactor = 1.0 / durationUnit.toNanos(1);
			this.durationUnit = durationUnit.toString().toLowerCase();
			;
			this.rateFactor = rateUnit.toSeconds(1);
			this.rateUnit = "events/" + calculateRateUnit(rateUnit);
		}

		@Override
		public double get50thPercentile() {
			return metric.getSnapshot().getMedian() * durationFactor;
		}

		@Override
		public double getMin() {
			return metric.getSnapshot().getMin() * durationFactor;
		}

		@Override
		public double getMax() {
			return metric.getSnapshot().getMax() * durationFactor;
		}

		@Override
		public double getMean() {
			return metric.getSnapshot().getMean() * durationFactor;
		}

		@Override
		public double getStdDev() {
			return metric.getSnapshot().getStdDev() * durationFactor;
		}

		@Override
		public double get75thPercentile() {
			return metric.getSnapshot().get75thPercentile() * durationFactor;
		}

		@Override
		public double get95thPercentile() {
			return metric.getSnapshot().get95thPercentile() * durationFactor;
		}

		@Override
		public double get98thPercentile() {
			return metric.getSnapshot().get98thPercentile() * durationFactor;
		}

		@Override
		public double get99thPercentile() {
			return metric.getSnapshot().get99thPercentile() * durationFactor;
		}

		@Override
		public double get999thPercentile() {
			return metric.getSnapshot().get999thPercentile() * durationFactor;
		}

		@Override
		public long[] values() {
			return metric.getSnapshot().getValues();
		}

		@Override
		public String getDurationUnit() {
			return durationUnit;
		}

		@Override
		public long getCount() {
			return metric.getCount();
		}

		@Override
		public double getMeanRate() {
			return metric.getMeanRate() * rateFactor;
		}

		@Override
		public double getOneMinuteRate() {
			return metric.getOneMinuteRate() * rateFactor;
		}

		@Override
		public double getFiveMinuteRate() {
			return metric.getFiveMinuteRate() * rateFactor;
		}

		@Override
		public double getFifteenMinuteRate() {
			return metric.getFifteenMinuteRate() * rateFactor;
		}

		@Override
		public String getRateUnit() {
			return rateUnit;
		}

		private String calculateRateUnit(TimeUnit unit) {
			final String s = unit.toString().toLowerCase();
			return s.substring(0, s.length() - 1);
		}
	}
}
