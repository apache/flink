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

package org.apache.flink.runtime.checkpoint.filemerging;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

import net.jcip.annotations.ThreadSafe;

/**
 * Metrics related to the file merging snapshot manager. Thread-safety is required because it is
 * used by multiple task threads.
 */
@ThreadSafe
public class FileMergingMetricGroup extends ProxyMetricGroup<MetricGroup> {

    private static final String PREFIX = "fileMerging";
    @VisibleForTesting public static final String LOGICAL_FILE_COUNT = PREFIX + ".logicalFileCount";
    @VisibleForTesting public static final String LOGICAL_FILE_SIZE = PREFIX + ".logicalFileSize";

    @VisibleForTesting
    public static final String PHYSICAL_FILE_COUNT = PREFIX + ".physicalFileCount";

    @VisibleForTesting public static final String PHYSICAL_FILE_SIZE = PREFIX + ".physicalFileSize";

    public FileMergingMetricGroup(
            MetricGroup parentMetricGroup, FileMergingSnapshotManager.SpaceStat spaceStat) {
        super(parentMetricGroup);
        registerMetrics(spaceStat);
    }

    public void registerMetrics(FileMergingSnapshotManager.SpaceStat spaceStat) {
        gauge(LOGICAL_FILE_COUNT, spaceStat.logicalFileCount::get);
        gauge(LOGICAL_FILE_SIZE, spaceStat.logicalFileSize::get);
        gauge(PHYSICAL_FILE_COUNT, spaceStat.physicalFileCount::get);
        gauge(PHYSICAL_FILE_SIZE, spaceStat.physicalFileSize::get);
    }
}
