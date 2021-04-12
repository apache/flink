/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.metrics;

import org.apache.flink.metrics.MetricGroup;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/** Tests for {@link PollingRecordPublisherMetricsReporter}. */
public class PollingRecordPublisherMetricsReporterTest {

    @InjectMocks private PollingRecordPublisherMetricsReporter metricsReporter;

    @Mock private MetricGroup metricGroup;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testMetricIdentifiers() {
        verify(metricGroup).gauge(eq(KinesisConsumerMetricConstants.BYTES_PER_READ), any());
        verify(metricGroup).gauge(eq(KinesisConsumerMetricConstants.LOOP_FREQUENCY_HZ), any());
        verify(metricGroup).gauge(eq(KinesisConsumerMetricConstants.MAX_RECORDS_PER_FETCH), any());
        verify(metricGroup).gauge(eq(KinesisConsumerMetricConstants.RUNTIME_LOOP_NANOS), any());
        verify(metricGroup).gauge(eq(KinesisConsumerMetricConstants.SLEEP_TIME_MILLIS), any());
    }

    @Test
    public void testGettersAndSetters() {
        metricsReporter.setBytesPerRead(1);
        metricsReporter.setLoopFrequencyHz(2);
        metricsReporter.setMaxNumberOfRecordsPerFetch(3);
        metricsReporter.setRunLoopTimeNanos(4);
        metricsReporter.setSleepTimeMillis(5);

        assertEquals(1, metricsReporter.getBytesPerRead(), 0);
        assertEquals(2, metricsReporter.getLoopFrequencyHz(), 0);
        assertEquals(3, metricsReporter.getMaxNumberOfRecordsPerFetch());
        assertEquals(4, metricsReporter.getRunLoopTimeNanos());
        assertEquals(5, metricsReporter.getSleepTimeMillis());
    }
}
