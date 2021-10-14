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

package org.apache.flink.python.metric;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.MetricGroupTest;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link FlinkMetricContainer}. */
public class FlinkMetricContainerTest {

    @Mock private RuntimeContext runtimeContext;
    @Mock private OperatorMetricGroup metricGroup;

    private FlinkMetricContainer container;

    private static final List<String> DEFAULT_SCOPE_COMPONENTS =
            Arrays.asList("key", "value", "MetricGroupType.key", "MetricGroupType.value");

    private static final String DEFAULT_NAMESPACE =
            "[\"key\", \"value\", \"MetricGroupType.key\", \"MetricGroupType.value\"]";

    @Before
    public void beforeTest() {
        MockitoAnnotations.initMocks(this);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(any(), any())).thenReturn(metricGroup);
        when(metricGroup.addGroup(any())).thenReturn(metricGroup);
        container = new FlinkMetricContainer(runtimeContext.getMetricGroup());
    }

    @Test
    public void testGetNameSpaceArray() {
        String json = "[\"key\", \"value\", \"MetricGroupType.key\", \"MetricGroupType.value\"]";
        MetricKey key = MetricKey.create("step", MetricName.named(json, "name"));
        assertThat(FlinkMetricContainer.getNameSpaceArray(key), is(DEFAULT_SCOPE_COMPONENTS));
    }

    @Test
    public void testGetFlinkMetricIdentifierString() {
        MetricKey key = MetricKey.create("step", MetricName.named(DEFAULT_NAMESPACE, "name"));
        assertThat(FlinkMetricContainer.getFlinkMetricIdentifierString(key), is("key.value.name"));
    }

    @Test
    public void testRegisterMetricGroup() {
        MetricKey key = MetricKey.create("step", MetricName.named(DEFAULT_NAMESPACE, "name"));

        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(
                        registry, new MetricGroupTest.DummyAbstractMetricGroup(registry), "root");
        MetricGroup metricGroup = FlinkMetricContainer.registerMetricGroup(key, root);

        assertThat(
                metricGroup.getScopeComponents(),
                is(Arrays.asList("root", "key", "value").toArray()));
    }

    @Test
    public void testCounterMonitoringInfoUpdate() {
        SimpleCounter userCounter = new SimpleCounter();
        when(metricGroup.counter("myCounter")).thenReturn(userCounter);

        MonitoringInfo userMonitoringInfo =
                new SimpleMonitoringInfoBuilder()
                        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, DEFAULT_NAMESPACE)
                        .setLabel(MonitoringInfoConstants.Labels.NAME, "myCounter")
                        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "anyPTransform")
                        .setInt64SumValue(111)
                        .build();

        assertThat(userCounter.getCount(), is(0L));
        container.updateMetrics("step", ImmutableList.of(userMonitoringInfo));
        assertThat(userCounter.getCount(), is(111L));
    }

    @Test
    public void testMeterMonitoringInfoUpdate() {
        MeterView userMeter = new MeterView(new SimpleCounter());
        when(metricGroup.meter(eq("myMeter"), any(Meter.class))).thenReturn(userMeter);
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
        assertThat(userMeter.getCount(), is(0L));
        assertThat(userMeter.getRate(), is(0.0));
        container.updateMetrics("step", ImmutableList.of(userMonitoringInfo));
        userMeter.update();
        assertThat(userMeter.getCount(), is(111L));
        assertThat(userMeter.getRate(), is(1.85)); // 111 div 60 = 1.85
    }

    @Test
    public void testGaugeMonitoringInfoUpdate() {
        MonitoringInfo userMonitoringInfo =
                new SimpleMonitoringInfoBuilder()
                        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, DEFAULT_NAMESPACE)
                        .setLabel(MonitoringInfoConstants.Labels.NAME, "myGauge")
                        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "anyPTransform")
                        .setInt64LatestValue(GaugeData.create(111L))
                        .build();

        container.updateMetrics("step", ImmutableList.of(userMonitoringInfo));
        verify(metricGroup)
                .gauge(
                        eq("myGauge"),
                        argThat(
                                (ArgumentMatcher<FlinkMetricContainer.FlinkGauge>)
                                        argument -> {
                                            Long actual = argument.getValue();
                                            return actual.equals(111L);
                                        }));
    }

    @Test
    public void testDistributionMonitoringInfoUpdate() {
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
        verify(metricGroup)
                .gauge(
                        eq("myDistribution"),
                        argThat(
                                (ArgumentMatcher<FlinkMetricContainer.FlinkDistributionGauge>)
                                        argument -> {
                                            DistributionResult actual = argument.getValue();
                                            DistributionResult expected =
                                                    DistributionResult.create(30, 10, 1, 5);
                                            return actual.equals(expected);
                                        }));
    }
}
