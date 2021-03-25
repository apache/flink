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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.changelog.StateChangelogWriterFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Filesystem-based implementation of {@link StateChangelogWriterFactory}. */
@Experimental
public class FsStateChangelogWriterFactory
        implements StateChangelogWriterFactory<StateChangelogHandleStreamImpl> {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogWriterFactory.class);

    /**
     * The log id is only needed on write to separate changes from different backends (i.e.
     * operators) in the resulting file.
     */
    private final AtomicInteger logIdGenerator = new AtomicInteger(0);

    private volatile StateChangeStore store;
    private volatile ReadableConfig config;

    /**
     * Creates {@link FsStateChangelogWriterFactory client} that implements batching and retries and
     * uses {@link DirectFsStateChangeStore} under the hood.
     *
     * @param basePath base path where to save the data
     * @param persistDelayMs how long to wait after the {@link StateChangelogWriter#persist persist}
     *     call and before actually persisting data. This is used for {@link
     *     BatchingStateChangeStore batching}
     * @param persistSizeThreshold when reached, persist is triggered regardless of persistDelayMs
     *     (number of pending state changes from any client)
     */
    public FsStateChangelogWriterFactory(
            Path basePath, long persistDelayMs, int persistSizeThreshold) throws IOException {
        this(
                StateChangeStore.createBatchingStore(
                        persistDelayMs,
                        persistSizeThreshold,
                        new DirectFsStateChangeStore(basePath, basePath.getFileSystem())));
    }

    /**
     * Creates a non-initialized factory to load via SPI. {@link #configure(ReadableConfig)} must be
     * called before use.
     */
    public FsStateChangelogWriterFactory() {}

    @Override
    public void configure(ReadableConfig config) {
        if (this.store != null) {
            Preconditions.checkState(this.config == config, "reconfiguration attempt");
            return;
        }
        try {
            this.store = StateChangeStore.fromConfig(config);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        this.config = config;
    }

    /**
     * Creates {@link FsStateChangelogWriterFactory client} that uses a simple {@link
     * DirectFsStateChangeStore} (i.e without any batching or retrying other than on FS level).
     */
    public FsStateChangelogWriterFactory(Path basePath) throws IOException {
        this(new DirectFsStateChangeStore(basePath, basePath.getFileSystem()));
    }

    /**
     * Creates {@link FsStateChangelogWriterFactory client} that uses a given {@link
     * StateChangeStore}.
     */
    public FsStateChangelogWriterFactory(StateChangeStore store) {
        this.store = store;
    }

    @Override
    public FsStateChangelogWriter createWriter(
            OperatorID operatorID,
            KeyGroupRange keyGroupRange,
            ChangelogCallbackExecutor executor) {
        Preconditions.checkState(store != null);
        UUID logId = new UUID(0, logIdGenerator.getAndIncrement());
        LOG.info("createWriter for operator {}/{}: {}", operatorID, keyGroupRange, logId);
        return new FsStateChangelogWriter(logId, keyGroupRange, store, executor);
    }

    @Override
    public void close() throws Exception {
        if (store != null) {
            store.close();
        }
    }
}
