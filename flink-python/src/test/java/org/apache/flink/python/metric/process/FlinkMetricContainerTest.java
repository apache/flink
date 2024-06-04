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

package org.apache.flink.python.metric.process;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.MetricGroupTest;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkMetricContainer}. */
class FlinkMetricContainerTest {

    private InterceptingOperatorMetricGroup metricGroup = new InterceptingOperatorMetricGroup();

    private FlinkMetricContainer container;

    private static final List<String> DEFAULT_SCOPE_COMPONENTS =
            Arrays.asList("key", "value", "MetricGroupType.key", "MetricGroupType.value");

    private static final String DEFAULT_NAMESPACE =
            "[\"key\", \"value\", \"MetricGroupType.key\", \"MetricGroupType.value\"]";

    @BeforeEach
    void beforeTest() {
        metricGroup =
                new InterceptingOperatorMetricGroup() {
                    @Override
                    public MetricGroup addGroup(int name) {
                        return this;
                    }

                    @Override
                    public MetricGroup addGroup(String name) {
                        return this;
                    }

                    @Override
                    public MetricGroup addGroup(String key, String value) {
                        return this;
                    }
                };
        container = new FlinkMetricContainer(metricGroup);
    }

    @Test
    void testGetNameSpaceArray() {
        String json = "[\"key\", \"value\", \"MetricGroupType.key\", \"MetricGroupType.value\"]";
        MetricKey key = MetricKey.create("step", MetricName.named(json, "name"));
        assertThat(FlinkMetricContainer.getNameSpaceArray(key)).isEqualTo(DEFAULT_SCOPE_COMPONENTS);
    }

    @Test
    void testGetFlinkMetricIdentifierString() {
        MetricKey key = MetricKey.create("step", MetricName.named(DEFAULT_NAMESPACE, "name"));
        assertThat(FlinkMetricContainer.getFlinkMetricIdentifierString(key))
                .isEqualTo("key.value.name");
    }

    @Test
    void testRegisterMetricGroup() {
        MetricKey key = MetricKey.create("step", MetricName.named(DEFAULT_NAMESPACE, "name"));

        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(
                        registry, new MetricGroupTest.DummyAbstractMetricGroup(registry), "root");
        MetricGroup metricGroup = FlinkMetricContainer.registerMetricGroup(key, root);

        assertThat(metricGroup.getScopeComponents())
                .isEqualTo(Arrays.asList("root", "key", "value").toArray());
    }

    @Test
    void testCounterMonitoringInfoUpdate() {
        MonitoringInfo userMonitoringInfo =
                new SimpleMonitoringInfoBuilder()
                        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, DEFAULT_NAMESPACE)
                        .setLabel(MonitoringInfoConstants.Labels.NAME, "myCounter")
                        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "anyPTransform")
                        .setInt64SumValue(111)
                        .build();

        assertThat(metricGroup.get("myCounter")).isNull();
        container.updateMetrics("step", ImmutableList.of(userMonitoringInfo));
        Counter userCounter = (Counter) metricGroup.get("myCounter");
        assertThat(userCounter.getCount()).isEqualTo(111L);
    }

    @Test
    void testMeterMonitoringInfoUpdate() {
        String namespace =
                "[\"key\", \"value\", \"MetricGroupType.key\", \"MetricGroupType.value\", \"60\"]";

        MonitoringInfo userMonitoringInfo =
                new SimpleMonitoringInfoBuilder()
                        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, namespace)
                        .setLabel(MonitoringInfoConstants.Labels.NAME, "myMeter")
                        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "anyPTransform")
                        .setInt64SumValue(111)
                        .build();

        assertThat(metricGroup.get("myMeter")).isNull();
        container.updateMetrics("step", ImmutableList.of(userMonitoringInfo));
        MeterView userMeter = (MeterView) metricGroup.get("myMeter");
        userMeter.update();
        assertThat(userMeter.getCount()).isEqualTo(111L);
        assertThat(userMeter.getRate()).isEqualTo(1.85); // 111 div 60 = 1.85
    }

    @Test
    void testGaugeMonitoringInfoUpdate() {
        MonitoringInfo userMonitoringInfo =
                new SimpleMonitoringInfoBuilder()
                        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, DEFAULT_NAMESPACE)
                        .setLabel(MonitoringInfoConstants.Labels.NAME, "myGauge")
                        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "anyPTransform")
                        .setInt64LatestValue(GaugeData.create(111L))
                        .build();

        container.updateMetrics("step", ImmutableList.of(userMonitoringInfo));
        FlinkMetricContainer.FlinkGauge myGauge =
                (FlinkMetricContainer.FlinkGauge) metricGroup.get("myGauge");

        assertThat(myGauge.getValue()).isEqualTo(111L);
    }

    @Test
    void testDistributionMonitoringInfoUpdate() {
        MonitoringInfo userMonitoringInfo =
                new SimpleMonitoringInfoBuilder()
                        .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
                        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, DEFAULT_NAMESPACE)
                        .setLabel(MonitoringInfoConstants.Labels.NAME, "myDistribution")
                        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "anyPTransform")
                        .setInt64DistributionValue(DistributionData.create(30, 10, 1, 5))
                        .build();

        container.updateMetrics("step", ImmutableList.of(userMonitoringInfo));
        // The one Flink distribution that gets created is a FlinkDistributionGauge; here we verify
        // its initial (and in this test, final) value
        FlinkMetricContainer.FlinkDistributionGauge myGauge =
                (FlinkMetricContainer.FlinkDistributionGauge) metricGroup.get("myDistribution");

        assertThat(myGauge.getValue()).isEqualTo(DistributionResult.create(30, 10, 1, 5));
    }
}
