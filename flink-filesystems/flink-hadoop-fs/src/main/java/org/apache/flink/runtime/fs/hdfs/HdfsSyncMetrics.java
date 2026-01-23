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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/** Registers HadoopDataOutputStream counters as Flink metrics. */
public final class HdfsSyncMetrics {
    private static final String MT_FLINK_METRIC_PREFIX = "mt_flink_";

    private HdfsSyncMetrics() {}

    public static void register(MetricGroup parentGroup) {
        MetricGroup g = parentGroup.addGroup("filesystem").addGroup("hdfs");

        g.gauge(
                createMTFlinkMetricName("hdfs_flush_calls"),
                (Gauge<Long>) HadoopDataOutputStream::getFlushCalls);

        g.gauge(
                createMTFlinkMetricName("hdfs_sync_calls"),
                (Gauge<Long>) HadoopDataOutputStream::getSyncCalls);

        g.gauge(
                createMTFlinkMetricName("recoverable_sync_calls"),
                (Gauge<Long>) BaseHadoopFsRecoverableFsDataOutputStream::getRecoverableSyncCalls);
    }

    public static String createMTFlinkMetricName(String metricName) {
        if (metricName == null) {
            throw new NullPointerException("metricName must not be null");
        }
        return MT_FLINK_METRIC_PREFIX + metricName;
    }
}
