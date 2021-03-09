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

import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.management.JMXService;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via JMX.
 *
 * <p>Largely based on the JmxReporter class of the dropwizard metrics library
 * https://github.com/dropwizard/metrics/blob/master/metrics-core/src/main/java/io/dropwizard/metrics/JmxReporter.java
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.jmx.JMXReporterFactory")
public class JMXReporter implements MetricReporter {

    static final String JMX_DOMAIN_PREFIX = "org.apache.flink.";

    private static final Logger LOG = LoggerFactory.getLogger(JMXReporter.class);

    private static final CharacterFilter CHARACTER_FILTER =
            new CharacterFilter() {
                @Override
                public String filterCharacters(String input) {
                    return replaceInvalidChars(input);
                }
            };

    // ------------------------------------------------------------------------

    /** The server where the management beans are registered and deregistered. */
    private final MBeanServer mBeanServer;

    /** The names under which the registered metrics have been added to the MBeanServer. */
    private final Map<Metric, ObjectName> registeredMetrics;

    JMXReporter(@Nullable final String portsConfig) {
        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
        this.registeredMetrics = new HashMap<>();

        if (portsConfig != null) {
            LOG.warn(
                    "JMXReporter port config is deprecated. " + "Please use: {} instead!",
                    JMXServerOptions.JMX_SERVER_PORT);
            JMXService.startInstance(portsConfig);
        }
    }

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {
        // Nothing to close.
    }

    public Optional<Integer> getPort() {
        return JMXService.getPort();
    }

    // ------------------------------------------------------------------------
    //  adding / removing metrics
    // ------------------------------------------------------------------------

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final String domain = generateJmxDomain(metricName, group);
        final Hashtable<String, String> table = generateJmxTable(group.getAllVariables());

        AbstractBean jmxMetric;
        ObjectName jmxName;
        try {
            jmxName = new ObjectName(domain, table);
        } catch (MalformedObjectNameException e) {
            /**
             * There is an implementation error on our side if this occurs. Either the domain was
             * modified and no longer conforms to the JMX domain rules or the table wasn't properly
             * generated.
             */
            LOG.debug(
                    "Implementation error. The domain or table does not conform to JMX rules.", e);
            return;
        }

        if (metric instanceof Gauge) {
            jmxMetric = new JmxGauge((Gauge<?>) metric);
        } else if (metric instanceof Counter) {
            jmxMetric = new JmxCounter((Counter) metric);
        } else if (metric instanceof Histogram) {
            jmxMetric = new JmxHistogram((Histogram) metric);
        } else if (metric instanceof Meter) {
            jmxMetric = new JmxMeter((Meter) metric);
        } else {
            LOG.error(
                    "Cannot add unknown metric type: {}. This indicates that the metric type "
                            + "is not supported by this reporter.",
                    metric.getClass().getName());
            return;
        }

        try {
            synchronized (this) {
                mBeanServer.registerMBean(jmxMetric, jmxName);
                registeredMetrics.put(metric, jmxName);
            }
        } catch (NotCompliantMBeanException e) {
            // implementation error on our side
            LOG.debug("Metric did not comply with JMX MBean rules.", e);
        } catch (InstanceAlreadyExistsException e) {
            LOG.warn("A metric with the name " + jmxName + " was already registered.", e);
        } catch (Throwable t) {
            LOG.warn("Failed to register metric", t);
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

    static Hashtable<String, String> generateJmxTable(Map<String, String> variables) {
        Hashtable<String, String> ht = new Hashtable<>(variables.size());
        for (Map.Entry<String, String> variable : variables.entrySet()) {
            ht.put(
                    replaceInvalidChars(variable.getKey()),
                    replaceInvalidChars(variable.getValue()));
        }
        return ht;
    }

    static String generateJmxDomain(String metricName, MetricGroup group) {
        return JMX_DOMAIN_PREFIX
                + ((FrontMetricGroup<AbstractMetricGroup<?>>) group)
                        .getLogicalScope(CHARACTER_FILTER, '.')
                + '.'
                + metricName;
    }

    /**
     * Lightweight method to replace unsupported characters. If the string does not contain any
     * unsupported characters, this method creates no new string (and in fact no new objects at
     * all).
     *
     * <p>Replacements:
     *
     * <ul>
     *   <li>{@code "} is removed
     *   <li>{@code space} is replaced by {@code _} (underscore)
     *   <li>{@code , = ; : ? ' *} are replaced by {@code -} (hyphen)
     * </ul>
     */
    static String replaceInvalidChars(String str) {
        char[] chars = null;
        final int strLen = str.length();
        int pos = 0;

        for (int i = 0; i < strLen; i++) {
            final char c = str.charAt(i);
            switch (c) {
                case '>':
                case '<':
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

    /** The common MBean interface for all metrics. */
    public interface MetricMBean {}

    private abstract static class AbstractBean implements MetricMBean {}

    /** The MBean interface for an exposed counter. */
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

    /** The MBean interface for an exposed gauge. */
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

    /** The MBean interface for an exposed histogram. */
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

    /** The MBean interface for an exposed meter. */
    public interface JmxMeterMBean extends MetricMBean {
        double getRate();

        long getCount();
    }

    private static class JmxMeter extends AbstractBean implements JmxMeterMBean {

        private final Meter meter;

        public JmxMeter(Meter meter) {
            this.meter = meter;
        }

        @Override
        public double getRate() {
            return meter.getRate();
        }

        @Override
        public long getCount() {
            return meter.getCount();
        }
    }
}
