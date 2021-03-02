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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleStreamHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PREEMPTIVE_PERSIST_THRESHOLD;
import static org.apache.flink.util.Preconditions.checkState;

/** Filesystem-based implementation of {@link StateChangelogStorage}. */
@Experimental
public class FsStateChangelogStorage
        implements StateChangelogStorage<ChangelogStateHandleStreamImpl>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogStorage.class);
    private static final long serialVersionUID = 1L;

    /**
     * The log id is only needed on write to separate changes from different backends (i.e.
     * operators) in the resulting file.
     */
    private transient AtomicInteger logIdGenerator = new AtomicInteger(0);

    private transient volatile StateChangeUploader uploader;
    private transient volatile FsStateChangelogCleaner cleaner;
    private volatile ReadableConfig config; // todo: make final after FLINK-21804

    /**
     * Creates a non-initialized factory to load via SPI. {@link #configure(ReadableConfig)} must be
     * called before use.
     */
    @SuppressWarnings("unused")
    public FsStateChangelogStorage() {}

    @Override
    public void configure(ReadableConfig config) {
        if (uploader != null) {
            checkState(config == this.config, "reconfiguration attempt");
            return;
        }
        try {
            cleaner = FsStateChangelogCleaner.fromConfig(config);
            uploader = StateChangeUploader.fromConfig(config, cleaner);
            logIdGenerator = new AtomicInteger(0);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        this.config = config;
    }

    /**
     * Creates {@link FsStateChangelogStorage client} that uses a simple {@link
     * StateChangeFsUploader} (i.e without any batching or retrying other than on FS level).
     */
    public FsStateChangelogStorage(
            Path basePath, boolean compression, int bufferSize, FsStateChangelogCleaner cleaner)
            throws IOException {
        this(
                new StateChangeFsUploader(
                        basePath, basePath.getFileSystem(), compression, bufferSize, cleaner),
                cleaner);
    }

    /**
     * Creates {@link FsStateChangelogStorage client} that uses a given {@link StateChangeUploader}.
     */
    public FsStateChangelogStorage(StateChangeUploader uploader, FsStateChangelogCleaner cleaner) {
        this.uploader = uploader;
        this.cleaner = cleaner;
        this.config = new Configuration();
    }

    @Override
    public FsStateChangelogWriter createWriter(String operatorID, KeyGroupRange keyGroupRange) {
        checkState(config != null);
        if (uploader == null) {
            synchronized (this) {
                if (uploader == null) {
                    configure(config);
                }
            }
        }
        UUID logId = new UUID(0, logIdGenerator.getAndIncrement());
        LOG.info("createWriter for operator {}/{}: {}", operatorID, keyGroupRange, logId);
        return new FsStateChangelogWriter(
                logId,
                keyGroupRange,
                uploader,
                config.get(PREEMPTIVE_PERSIST_THRESHOLD).getBytes());
    }

    @Override
    public StateChangelogHandleReader<ChangelogStateHandleStreamImpl> createReader() {
        return new StateChangelogHandleStreamHandleReader(new StateChangeFormat());
    }

    @Override
    public void close() throws Exception {
        if (uploader != null) {
            uploader.close();
        }
    }
}
