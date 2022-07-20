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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.NUM_DISCARD_THREADS;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PREEMPTIVE_PERSIST_THRESHOLD;
import static org.apache.flink.changelog.fs.StateChangeUploadScheduler.directScheduler;
import static org.apache.flink.changelog.fs.StateChangeUploadScheduler.fromConfig;
import static org.apache.flink.changelog.fs.TaskChangelogRegistry.defaultChangelogRegistry;

/** Filesystem-based implementation of {@link StateChangelogStorage}. */
@Experimental
@ThreadSafe
public class FsStateChangelogStorage extends FsStateChangelogStorageForRecovery
        implements StateChangelogStorage<ChangelogStateHandleStreamImpl> {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogStorage.class);

    private final StateChangeUploadScheduler uploader;
    private final long preEmptivePersistThresholdInBytes;

    /**
     * The log id is only needed on write to separate changes from different backends (i.e.
     * operators) in the resulting file.
     */
    private final AtomicInteger logIdGenerator = new AtomicInteger(0);

    private final TaskChangelogRegistry changelogRegistry;

    public FsStateChangelogStorage(
            JobID jobID, Configuration config, TaskManagerJobMetricGroup metricGroup)
            throws IOException {
        this(jobID, config, metricGroup, defaultChangelogRegistry(config.get(NUM_DISCARD_THREADS)));
    }

    public FsStateChangelogStorage(
            JobID jobID,
            Configuration config,
            TaskManagerJobMetricGroup metricGroup,
            TaskChangelogRegistry changelogRegistry)
            throws IOException {
        this(
                fromConfig(
                        jobID,
                        config,
                        new ChangelogStorageMetricGroup(metricGroup),
                        changelogRegistry),
                config.get(PREEMPTIVE_PERSIST_THRESHOLD).getBytes(),
                changelogRegistry);
    }

    @VisibleForTesting
    public FsStateChangelogStorage(
            JobID jobID,
            Path basePath,
            boolean compression,
            int bufferSize,
            ChangelogStorageMetricGroup metricGroup,
            TaskChangelogRegistry changelogRegistry)
            throws IOException {
        this(
                directScheduler(
                        new StateChangeFsUploader(
                                jobID,
                                basePath,
                                basePath.getFileSystem(),
                                compression,
                                bufferSize,
                                metricGroup,
                                changelogRegistry)),
                PREEMPTIVE_PERSIST_THRESHOLD.defaultValue().getBytes(),
                changelogRegistry);
    }

    @VisibleForTesting
    public FsStateChangelogStorage(
            StateChangeUploadScheduler uploader,
            long preEmptivePersistThresholdInBytes,
            TaskChangelogRegistry changelogRegistry) {
        this.preEmptivePersistThresholdInBytes = preEmptivePersistThresholdInBytes;
        this.changelogRegistry = changelogRegistry;
        this.uploader = uploader;
    }

    @Override
    public FsStateChangelogWriter createWriter(
            String operatorID, KeyGroupRange keyGroupRange, MailboxExecutor mailboxExecutor) {
        UUID logId = new UUID(0, logIdGenerator.getAndIncrement());
        LOG.info("createWriter for operator {}/{}: {}", operatorID, keyGroupRange, logId);
        return new FsStateChangelogWriter(
                logId,
                keyGroupRange,
                uploader,
                preEmptivePersistThresholdInBytes,
                mailboxExecutor,
                changelogRegistry);
    }

    @Override
    public void close() throws Exception {
        uploader.close();
    }

    @Override
    public AvailabilityProvider getAvailabilityProvider() {
        return uploader.getAvailabilityProvider();
    }
}
