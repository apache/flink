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

package org.apache.flink.changelog.fs;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Metrics related to the Changelog Storage used by the Changelog State Backend. Thread-safety is
 * required because it is used by multiple uploader threads.
 */
@ThreadSafe
public class ChangelogStorageMetricGroup extends ProxyMetricGroup<MetricGroup> {
    private static final int WINDOW_SIZE = 1000;

    private final Counter uploadsCounter;
    private final Counter uploadFailuresCounter;
    private final Histogram uploadBatchSizes;
    private final Histogram uploadSizes;
    private final Histogram uploadLatenciesNanos;
    private final Histogram attemptsPerUpload;
    private final Histogram totalAttemptsPerUpload;

    public ChangelogStorageMetricGroup(MetricGroup parent) {
        super(parent);
        this.uploadsCounter =
                counter(CHANGELOG_STORAGE_NUM_UPLOAD_REQUESTS, new ThreadSafeSimpleCounter());
        this.uploadBatchSizes =
                histogram(
                        CHANGELOG_STORAGE_UPLOAD_BATCH_SIZES,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        this.attemptsPerUpload =
                histogram(
                        CHANGELOG_STORAGE_ATTEMPTS_PER_UPLOAD,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        this.totalAttemptsPerUpload =
                histogram(
                        CHANGELOG_STORAGE_TOTAL_ATTEMPTS_PER_UPLOAD,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        this.uploadSizes =
                histogram(
                        CHANGELOG_STORAGE_UPLOAD_SIZES,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        this.uploadLatenciesNanos =
                histogram(
                        CHANGELOG_STORAGE_UPLOAD_LATENCIES_NANOS,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
        this.uploadFailuresCounter =
                counter(CHANGELOG_STORAGE_NUM_UPLOAD_FAILURES, new ThreadSafeSimpleCounter());
    }

    public Counter getUploadsCounter() {
        return uploadsCounter;
    }

    public Counter getUploadFailuresCounter() {
        return uploadFailuresCounter;
    }

    public Histogram getAttemptsPerUpload() {
        return attemptsPerUpload;
    }

    public Histogram getTotalAttemptsPerUpload() {
        return totalAttemptsPerUpload;
    }

    /**
     * The number of upload tasks (coming from one or more writers, i.e. backends/tasks) that were
     * grouped together and form a single upload resulting in a single file.
     */
    public Histogram getUploadBatchSizes() {
        return uploadBatchSizes;
    }

    public Histogram getUploadSizes() {
        return uploadSizes;
    }

    public Histogram getUploadLatenciesNanos() {
        return uploadLatenciesNanos;
    }

    public void registerUploadQueueSizeGauge(Gauge<Integer> gauge) {
        gauge(CHANGELOG_STORAGE_UPLOAD_QUEUE_SIZE, gauge);
    }

    private static final String PREFIX = "ChangelogStorage";
    public static final String CHANGELOG_STORAGE_NUM_UPLOAD_REQUESTS =
            PREFIX + ".numberOfUploadRequests";
    public static final String CHANGELOG_STORAGE_NUM_UPLOAD_FAILURES =
            PREFIX + ".numberOfUploadFailures";
    public static final String CHANGELOG_STORAGE_UPLOAD_SIZES = PREFIX + ".uploadSizes";
    public static final String CHANGELOG_STORAGE_UPLOAD_LATENCIES_NANOS =
            PREFIX + ".uploadLatenciesNanos";
    public static final String CHANGELOG_STORAGE_ATTEMPTS_PER_UPLOAD =
            PREFIX + ".attemptsPerUpload";
    public static final String CHANGELOG_STORAGE_TOTAL_ATTEMPTS_PER_UPLOAD =
            PREFIX + ".totalAttemptsPerUpload";
    public static final String CHANGELOG_STORAGE_UPLOAD_BATCH_SIZES = PREFIX + ".uploadBatchSizes";
    public static final String CHANGELOG_STORAGE_UPLOAD_QUEUE_SIZE = PREFIX + ".uploadQueueSize";
}
