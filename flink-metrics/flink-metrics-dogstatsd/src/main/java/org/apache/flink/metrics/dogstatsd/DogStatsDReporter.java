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

package org.apache.flink.metrics.dogstatsd;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Metrics reporter for DogStatsD protocol, inspired by DatadogHttpReporter, StatsDReporter and the
 * work in FLINK-7009.
 */
@InstantiateViaFactory(
        factoryClassName = "org.apache.flink.metrics.dogstatsd.DogStatsDReporterFactory")
public class DogStatsDReporter implements MetricReporter, Scheduled, CharacterFilter {

    enum DMetricType {
        COUNTER("c"),
        GAUGE("g");

        final String typeEncoding;

        private DMetricType(String typeEncoding) {
            this.typeEncoding = typeEncoding;
        }
    }

    static class DMetric {
        private final String name;
        private final List<String> tags;

        DMetric(String name, List<String> tags) {
            this.name = name;
            this.tags = tags;
        }

        String getName() {
            return name;
        }

        List<String> getTags() {
            return tags;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(DogStatsDReporter.class);

    private static final String ARG_HOST = "host";
    private static final String ARG_PORT = "port";
    private static final String ARG_TAGS = "tags";
    private static final String ARG_SHORTIDS = "shortids";

    private boolean closed = false;

    private DatagramSocket socket;
    private InetSocketAddress address;

    private List<String> configTags;
    private boolean shortIds;

    private final Pattern instanceRef = Pattern.compile("@[a-f0-9]+");
    private final Pattern flinkId = Pattern.compile("[a-f0-9]{32}");
    private static final char SCOPE_SEPARATOR = '.';

    protected final Map<Gauge<?>, DMetric> gauges = new ConcurrentHashMap<>();
    protected final Map<Counter, DMetric> counters = new ConcurrentHashMap<>();
    protected final Map<Histogram, DMetric> histograms = new ConcurrentHashMap<>();
    protected final Map<Meter, DMetric> meters = new ConcurrentHashMap<>();

    @Override
    public void open(MetricConfig config) {
        String host = config.getString(ARG_HOST, null);
        int port = config.getInteger(ARG_PORT, 8125);
        String tags = config.getString(ARG_TAGS, "");

        configTags = getTagsFromConfig(tags);
        shortIds = config.getBoolean(ARG_SHORTIDS, false);

        if (host == null || host.length() == 0 || port < 1) {
            throw new IllegalArgumentException(
                    "Invalid host/port configuration. Host: " + host + " Port: " + port);
        }

        establishConnection(host, port);

        LOG.info("Configured DogStatsDReporter with config: {}", config);
    }

    protected void establishConnection(String host, Integer port) {
        this.address = new InetSocketAddress(host, port);

        try {
            this.socket = new DatagramSocket(0);
        } catch (SocketException e) {
            throw new RuntimeException("Could not create datagram socket. ", e);
        }
    }

    @Override
    public void close() {
        closed = true;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        String name = group.getMetricIdentifier(metricName, this);

        List<String> tags = new ArrayList<>(configTags);
        tags.addAll(getTagsFromMetricGroup(group));

        DMetric dMetric = new DMetric(name, tags);

        if (metric instanceof Counter) {
            counters.put((Counter) metric, dMetric);
        } else if (metric instanceof Gauge) {
            gauges.put((Gauge<?>) metric, dMetric);
        } else if (metric instanceof Histogram) {
            histograms.put((Histogram) metric, dMetric);
        } else if (metric instanceof Meter) {
            meters.put((Meter) metric, dMetric);
        } else {
            LOG.warn(
                    "Cannot add unknown metric type {}. This indicates that the reporter "
                            + "does not support this metric type.",
                    metric.getClass().getName());
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        if (metric instanceof Counter) {
            counters.remove(metric);
        } else if (metric instanceof Gauge) {
            gauges.remove(metric);
        } else if (metric instanceof Histogram) {
            histograms.remove(metric);
        } else if (metric instanceof Meter) {
            meters.remove(metric);
        } else {
            LOG.warn(
                    "Cannot remove unknown metric type {}. This indicates that the reporter "
                            + "does not support this metric type.",
                    metric.getClass().getName());
        }
    }

    @Override
    public void report() {
        // instead of locking here, we tolerate exceptions
        // we do this to prevent holding the lock for very long and blocking
        // operator creation and shutdown
        try {
            for (Map.Entry<Gauge<?>, DMetric> entry : gauges.entrySet()) {
                if (closed) {
                    return;
                }
                reportGauge(entry.getValue(), entry.getKey());
            }

            for (Map.Entry<Counter, DMetric> entry : counters.entrySet()) {
                if (closed) {
                    return;
                }
                reportCounter(entry.getValue(), entry.getKey());
            }

            for (Map.Entry<Histogram, DMetric> entry : histograms.entrySet()) {
                if (closed) {
                    return;
                }
                reportHistogram(entry.getValue(), entry.getKey());
            }

            for (Map.Entry<Meter, DMetric> entry : meters.entrySet()) {
                if (closed) {
                    return;
                }
                reportMeter(entry.getValue(), entry.getKey());
            }
        } catch (ConcurrentModificationException | NoSuchElementException e) {
            // ignore - may happen when metrics are concurrently added or removed
            // report next time
        }
    }

    // ------------------------------------------------------------------------

    private void reportCounter(final DMetric metric, final Counter counter) {
        send(metric.getName(), counter.getCount(), DMetricType.COUNTER, metric.getTags());
    }

    private void reportGauge(final DMetric metric, final Gauge<?> gauge) {
        Object value = gauge.getValue();
        if (value == null) {
            return;
        }

        // enforce numeric values and skip values like "n/a", "false", etc.
        if (value instanceof Number) {
            send(metric.getName(), value.toString(), DMetricType.GAUGE, metric.getTags());
        } else {
            LOG.debug(
                    "Metric [{}] will not be reported as the value type is not a number. Only number types are supported by this reporter",
                    metric.getName());
        }
    }

    private void reportHistogram(final DMetric metric, final Histogram histogram) {
        if (histogram != null) {
            HistogramStatistics statistics = histogram.getStatistics();

            if (statistics != null) {
                // this selection is based on
                // https://docs.datadoghq.com/developers/metrics/types/?tab=histogram
                // we only exclude 'sum' (which is optional), because we cannot compute it
                // the semantics for count are also slightly different, because we don't reset it
                // after a
                // report
                send(
                        prefix(metric.getName(), "count"),
                        histogram.getCount(),
                        DMetricType.COUNTER,
                        metric.getTags());
                send(
                        prefix(metric.getName(), "max"),
                        statistics.getMax(),
                        DMetricType.GAUGE,
                        metric.getTags());
                send(
                        prefix(metric.getName(), "min"),
                        statistics.getMin(),
                        DMetricType.GAUGE,
                        metric.getTags());
                send(
                        prefix(metric.getName(), "avg"),
                        statistics.getMean(),
                        DMetricType.GAUGE,
                        metric.getTags());
                send(
                        prefix(metric.getName(), "median"),
                        statistics.getQuantile(0.50),
                        DMetricType.GAUGE,
                        metric.getTags());
                send(
                        prefix(metric.getName(), "95percentile"),
                        statistics.getQuantile(0.95),
                        DMetricType.GAUGE,
                        metric.getTags());
            }
        }
    }

    private void reportMeter(final DMetric metric, final Meter meter) {
        if (meter != null) {
            send(
                    prefix(metric.getName(), "rate"),
                    meter.getRate(),
                    DMetricType.GAUGE,
                    metric.getTags());
            send(
                    prefix(metric.getName(), "count"),
                    meter.getCount(),
                    DMetricType.COUNTER,
                    metric.getTags());
        }
    }

    private String prefix(String... names) {
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

    private void send(String name, double value, DMetricType type, List<String> tags) {
        send(name, String.valueOf(value), type, tags);
    }

    private void send(String name, long value, DMetricType type, List<String> tags) {
        send(name, String.valueOf(value), type, tags);
    }

    private void send(
            final String name,
            final String value,
            final DMetricType type,
            final List<String> tags) {
        String typeString = type.typeEncoding;
        String tagsString = tags.isEmpty() ? "" : "|#" + String.join(",", tags);

        String formatted = String.format("%s:%s|%s%s", name, value, typeString, tagsString);

        try {
            byte[] data = formatted.getBytes(StandardCharsets.UTF_8);
            socket.send(new DatagramPacket(data, data.length, this.address));
        } catch (IOException e) {
            LOG.error(
                    "Unable to send packet to DogStatsD at '{}:{}'",
                    address.getHostName(),
                    address.getPort());
        }
    }

    /** Get tags from MetricGroup#getAllVariables(). */
    private List<String> getTagsFromMetricGroup(MetricGroup metricGroup) {
        List<String> tags = new ArrayList<>();

        for (Map.Entry<String, String> entry : metricGroup.getAllVariables().entrySet()) {
            tags.add(stripBrackets(entry.getKey()) + ":" + entry.getValue());
        }
        return tags;
    }

    /** Removes leading and trailing angle brackets. */
    private String stripBrackets(String str) {
        return str.substring(1, str.length() - 1);
    }

    private List<String> getTagsFromConfig(String str) {
        if (str.trim().isEmpty()) {
            return new ArrayList<>();
        } else {
            return Arrays.asList(str.split(","));
        }
    }

    /**
     * DogStatsD names should: start with letter, uses ascii alphanumerics and underscore, separated
     * by periods. Collapse runs of invalid characters into an underscore. Discard invalid prefix
     * and suffix. Eg: ":::metric:::name:::" -> "metric_name"
     */
    private boolean isValidStatsdChar(char c) {
        return (c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9')
                || (c == '_')
                || (c == '.');
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
     * <p>We might get a string that has coded structures, references to instances of serializers
     * and reducers, and even if we normalize all the odd characters could be overly long for a
     * metric name, likely to be truncated downstream. Our choices here appear to be either to
     * discard invalid metrics, or to pragmatically handle each of the various issues and produce
     * something that might be useful in aggregate even though the named parts are hard to read.
     *
     * <p>This function will find and remove all object references like @abcd0123, so that task and
     * operator names are stable. The name of an operator should be the same every time it is run,
     * so we should ignore object hash ids like these.
     *
     * <p>If the segment is a tm_id, task_id, job_id, task_attempt_id, we can optionally trim those
     * to the first 8 chars. This can reduce overall length substantially while still preserving
     * enough to distinguish metrics from each other.
     *
     * <p>If the segment is 50 chars or longer, we will compress it to avoid truncation. The
     * compression will look like the first 10 valid chars followed by a hash of the original. This
     * sacrifices readability for utility as a metric, so that latency metrics might survive with
     * valid and useful dimensions for aggregation, even if it is very hard to reverse engineer the
     * particular operator name. Application developers can of course still supply their own names
     * and are not forced to rely on the defaults.
     *
     * <p>This will turn something like: "TriggerWindow(TumblingProcessingTimeWindows(5000),
     * ReducingStateDescriptor{serializer=org.apache.flink.api.java
     * .typeutils.runtime.PojoSerializer@f3395ffa,
     * reduceFunction=org.apache.flink.streaming.examples.socket. SocketWindowWordCount$1@4201c465},
     * ProcessingTimeTrigger(), WindowedStream.reduce(WindowedStream.java-301))"
     *
     * <p>into: "TriggerWin_c2910b88"
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
