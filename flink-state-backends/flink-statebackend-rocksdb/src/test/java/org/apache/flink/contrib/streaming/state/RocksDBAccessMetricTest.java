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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link RocksDBAccessMetric}. */
public class RocksDBAccessMetricTest {
    @Test
    public void testRocksDBAccessMetricBuild() {
        RocksDBAccessMetric.Builder builder = new RocksDBAccessMetric.Builder();
        assertNull(builder.build());

        builder.setEnabled(true);
        builder.setMetricGroup(new UnregisteredMetricsGroup());
        builder.setSampleInterval(200);
        builder.setHistogramSlidingWindow(Time.seconds(5).toMilliseconds());

        RocksDBAccessMetric rocksDBAccessMetric = builder.build();
        assertEquals(5 * 1000L, rocksDBAccessMetric.getHistogramWindowSize());
        assertEquals(200, rocksDBAccessMetric.getSampleCountInterval());
    }

    @Test
    public void testRocksDBAccessMetricBuilderFromConfig() {
        Configuration configuration = new Configuration();
        RocksDBAccessMetric.Builder builder = RocksDBAccessMetric.builderFromConfig(configuration);
        // default implementation.
        assertNull(builder.build());

        configuration.setBoolean(RocksDBOptions.LATENCY_TRACK_ENABLED, true);
        builder = RocksDBAccessMetric.builderFromConfig(configuration);
        builder.setMetricGroup(new UnregisteredMetricsGroup());
        RocksDBAccessMetric dbAccessMetric = builder.build();
        assertTrue(dbAccessMetric.isMetricsSampleEnabled());
        assertEquals(
                (int) RocksDBOptions.LATENCY_TRACK_SAMPLE_INTERVAL.defaultValue(),
                dbAccessMetric.getSampleCountInterval());
        assertEquals(
                (long) RocksDBOptions.LATENCY_TRACK_SLIDING_WINDOW.defaultValue(),
                dbAccessMetric.getHistogramWindowSize());

        configuration.setInteger(RocksDBOptions.LATENCY_TRACK_SAMPLE_INTERVAL, 0);
        builder = RocksDBAccessMetric.builderFromConfig(configuration);
        builder.setMetricGroup(new UnregisteredMetricsGroup());
        assertFalse(builder.build().isMetricsSampleEnabled());
    }
}
