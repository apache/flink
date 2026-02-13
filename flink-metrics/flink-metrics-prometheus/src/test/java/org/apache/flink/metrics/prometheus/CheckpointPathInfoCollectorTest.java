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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.metrics.Gauge;

import io.prometheus.client.Collector;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AbstractPrometheusReporter.CheckpointPathInfoCollector}. */
class CheckpointPathInfoCollectorTest {

    @Test
    void testCheckpointPathExportedAsInfoMetric() {
        // Given: A checkpoint path gauge
        final String checkpointPath = "hdfs://namenode:8020/flink/checkpoints/job123/chk-456";
        Gauge<String> pathGauge = () -> checkpointPath;

        List<String> labelNames = Arrays.asList("host", "job_id");
        List<String> labelValues = Arrays.asList("localhost", "test_job");

        // When: Creating the collector
        AbstractPrometheusReporter.CheckpointPathInfoCollector collector =
                new AbstractPrometheusReporter.CheckpointPathInfoCollector(
                        pathGauge, "test_metric_info", "Test metric", labelNames, labelValues);

        // Then: Collect samples
        List<Collector.MetricFamilySamples> samples = collector.collect();

        assertThat(samples).hasSize(1);
        Collector.MetricFamilySamples family = samples.get(0);
        assertThat(family.name).isEqualTo("test_metric_info");
        assertThat(family.type).isEqualTo(Collector.Type.GAUGE);
        assertThat(family.samples).hasSize(1);

        Collector.MetricFamilySamples.Sample sample = family.samples.get(0);
        assertThat(sample.name).isEqualTo("test_metric_info");
        assertThat(sample.value).isEqualTo(1.0);

        // Verify labels include the path
        assertThat(sample.labelNames).contains("path");
        assertThat(sample.labelValues).contains(checkpointPath);
        assertThat(sample.labelNames).contains("host");
        assertThat(sample.labelValues).contains("localhost");
        assertThat(sample.labelNames).contains("job_id");
        assertThat(sample.labelValues).contains("test_job");
    }

    @Test
    void testNullCheckpointPathReturnsEmptyList() {
        // Given: A gauge returning null
        Gauge<String> pathGauge = () -> null;

        // When: Creating the collector
        AbstractPrometheusReporter.CheckpointPathInfoCollector collector =
                new AbstractPrometheusReporter.CheckpointPathInfoCollector(
                        pathGauge,
                        "test_metric_info",
                        "Test metric",
                        Collections.emptyList(),
                        Collections.emptyList());

        // Then: Should return empty list
        List<Collector.MetricFamilySamples> samples = collector.collect();
        assertThat(samples).isEmpty();
    }

    @Test
    void testEmptyCheckpointPathReturnsEmptyList() {
        // Given: A gauge returning empty string
        Gauge<String> pathGauge = () -> "";

        // When: Creating the collector
        AbstractPrometheusReporter.CheckpointPathInfoCollector collector =
                new AbstractPrometheusReporter.CheckpointPathInfoCollector(
                        pathGauge,
                        "test_metric_info",
                        "Test metric",
                        Collections.emptyList(),
                        Collections.emptyList());

        // Then: Should return empty list
        List<Collector.MetricFamilySamples> samples = collector.collect();
        assertThat(samples).isEmpty();
    }

    @Test
    void testCheckpointPathWithSpecialCharacters() {
        // Given: A checkpoint path with special characters
        final String checkpointPath =
                "s3://my-bucket/flink/checkpoints/job_123/chk-456?version=1";
        Gauge<String> pathGauge = () -> checkpointPath;

        List<String> labelNames = Arrays.asList("job_name");
        List<String> labelValues = Arrays.asList("my_job");

        // When: Creating the collector
        AbstractPrometheusReporter.CheckpointPathInfoCollector collector =
                new AbstractPrometheusReporter.CheckpointPathInfoCollector(
                        pathGauge, "test_metric_info", "Test metric", labelNames, labelValues);

        // Then: Path should be preserved as-is in the label
        List<Collector.MetricFamilySamples> samples = collector.collect();
        assertThat(samples).hasSize(1);

        Collector.MetricFamilySamples.Sample sample = samples.get(0).samples.get(0);
        assertThat(sample.labelValues).contains(checkpointPath);
    }
}
