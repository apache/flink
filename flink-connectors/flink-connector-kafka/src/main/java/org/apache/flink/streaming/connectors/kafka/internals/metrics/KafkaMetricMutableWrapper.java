/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;

import org.apache.kafka.common.Metric;

/** Gauge for getting the current value of a Kafka metric. */
@Internal
public class KafkaMetricMutableWrapper implements Gauge<Double> {
    private Metric kafkaMetric;

    public KafkaMetricMutableWrapper(Metric metric) {
        this.kafkaMetric = metric;
    }

    @Override
    public Double getValue() {
        return kafkaMetric.value();
    }

    public void setKafkaMetric(Metric kafkaMetric) {
        this.kafkaMetric = kafkaMetric;
    }
}
