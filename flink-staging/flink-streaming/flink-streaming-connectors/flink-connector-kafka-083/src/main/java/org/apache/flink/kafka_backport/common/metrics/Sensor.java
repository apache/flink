/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.kafka_backport.common.metrics;

import org.apache.flink.kafka_backport.common.MetricName;
import org.apache.flink.kafka_backport.common.utils.Time;
import org.apache.flink.kafka_backport.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

/**
 * A sensor applies a continuous sequence of numerical values to a set of associated metrics. For example a sensor on
 * message size would record a sequence of message sizes using the {@link #record(double)} api and would maintain a set
 * of metrics about request sizes such as the average or max.
 */
public final class Sensor {

    private final Metrics registry;
    private final String name;
    private final Sensor[] parents;
    private final List<Stat> stats;
    private final List<KafkaMetric> metrics;
    private final MetricConfig config;
    private final Time time;

    Sensor(Metrics registry, String name, Sensor[] parents, MetricConfig config, Time time) {
        super();
        this.registry = registry;
        this.name = Utils.notNull(name);
        this.parents = parents == null ? new Sensor[0] : parents;
        this.metrics = new ArrayList<KafkaMetric>();
        this.stats = new ArrayList<Stat>();
        this.config = config;
        this.time = time;
        checkForest(new HashSet<Sensor>());
    }

    /* Validate that this sensor doesn't end up referencing itself */
    private void checkForest(Set<Sensor> sensors) {
        if (!sensors.add(this))
            throw new IllegalArgumentException("Circular dependency in sensors: " + name() + " is its own parent.");
        for (int i = 0; i < parents.length; i++)
            parents[i].checkForest(sensors);
    }

    /**
     * The name this sensor is registered with. This name will be unique among all registered sensors.
     */
    public String name() {
        return this.name;
    }

    /**
     * Record an occurrence, this is just short-hand for {@link #record(double) record(1.0)}
     */
    public void record() {
        record(1.0);
    }

    /**
     * Record a value with this sensor
     * @param value The value to record
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double value) {
        record(value, time.milliseconds());
    }

    /**
     * Record a value at a known time. This method is slightly faster than {@link #record(double)} since it will reuse
     * the time stamp.
     * @param value The value we are recording
     * @param timeMs The current POSIX time in milliseconds
     * @throws QuotaViolationException if recording this value moves a metric beyond its configured maximum or minimum
     *         bound
     */
    public void record(double value, long timeMs) {
        synchronized (this) {
            // increment all the stats
            for (int i = 0; i < this.stats.size(); i++)
                this.stats.get(i).record(config, value, timeMs);
            checkQuotas(timeMs);
        }
        for (int i = 0; i < parents.length; i++)
            parents[i].record(value, timeMs);
    }

    /**
     * Check if we have violated our quota for any metric that has a configured quota
     * @param timeMs
     */
    private void checkQuotas(long timeMs) {
        for (int i = 0; i < this.metrics.size(); i++) {
            KafkaMetric metric = this.metrics.get(i);
            MetricConfig config = metric.config();
            if (config != null) {
                Quota quota = config.quota();
                if (quota != null) {
                    if (!quota.acceptable(metric.value(timeMs)))
                        throw new QuotaViolationException(metric.metricName() + " is in violation of its quota of " + quota.bound());
                }
            }
        }
    }

    /**
     * Register a compound statistic with this sensor with no config override
     */
    public void add(CompoundStat stat) {
        add(stat, null);
    }

    /**
     * Register a compound statistic with this sensor which yields multiple measurable quantities (like a histogram)
     * @param stat The stat to register
     * @param config The configuration for this stat. If null then the stat will use the default configuration for this
     *        sensor.
     */
    public synchronized void add(CompoundStat stat, MetricConfig config) {
        this.stats.add(Utils.notNull(stat));
        for (CompoundStat.NamedMeasurable m : stat.stats()) {
            KafkaMetric metric = new KafkaMetric(this, m.name(), m.stat(), config == null ? this.config : config, time);
            this.registry.registerMetric(metric);
            this.metrics.add(metric);
        }
    }

    /**
     * Register a metric with this sensor
     * @param metricName The name of the metric
     * @param stat The statistic to keep
     */
    public void add(MetricName metricName, MeasurableStat stat) {
        add(metricName, stat, null);
    }

    /**
     * Register a metric with this sensor
     * @param metricName The name of the metric
     * @param stat The statistic to keep
     * @param config A special configuration for this metric. If null use the sensor default configuration.
     */
    public synchronized void add(MetricName metricName, MeasurableStat stat, MetricConfig config) {
        KafkaMetric metric = new KafkaMetric(new Object(),
                                             Utils.notNull(metricName),
                                             Utils.notNull(stat),
                                             config == null ? this.config : config,
                                             time);
        this.registry.registerMetric(metric);
        this.metrics.add(metric);
        this.stats.add(stat);
    }

    synchronized List<KafkaMetric> metrics() {
        return Collections.unmodifiableList(this.metrics);
    }

}
