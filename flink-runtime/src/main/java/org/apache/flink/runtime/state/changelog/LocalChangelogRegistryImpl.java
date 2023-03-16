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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

@Internal
public class LocalChangelogRegistryImpl implements LocalChangelogRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(LocalChangelogRegistry.class);
    /**
     * All registered handles. (PhysicalStateHandleID, (handle, checkpointID)) represents a handle
     * and the latest checkpoint that refer to this handle.
     */
    private final Map<PhysicalStateHandleID, Tuple2<StreamStateHandle, Long>>
            handleToLastUsedCheckpointID = new ConcurrentHashMap<>();

    /** Executor for async state deletion. */
    private final ExecutorService asyncDisposalExecutor;

    public LocalChangelogRegistryImpl(ExecutorService ioExecutor) {
        this.asyncDisposalExecutor = ioExecutor;
    }

    public void register(StreamStateHandle handle, long checkpointID) {
        handleToLastUsedCheckpointID.compute(
                handle.getStreamStateHandleID(),
                (k, v) -> {
                    if (v == null) {
                        return Tuple2.of(handle, checkpointID);
                    } else {
                        Preconditions.checkState(handle.equals(v.f0));
                        return Tuple2.of(handle, Math.max(v.f1, checkpointID));
                    }
                });
    }

    public void discardUpToCheckpoint(long upTo) {
        List<StreamStateHandle> handles = new ArrayList<>();
        synchronized (handleToLastUsedCheckpointID) {
            Iterator<Tuple2<StreamStateHandle, Long>> iterator =
                    handleToLastUsedCheckpointID.values().iterator();
            while (iterator.hasNext()) {
                Tuple2<StreamStateHandle, Long> entry = iterator.next();
                if (entry.f1 < upTo) {
                    handles.add(entry.f0);
                    iterator.remove();
                }
            }
        }
        for (StreamStateHandle handle : handles) {
            scheduleAsyncDelete(handle);
        }
    }

    private void scheduleAsyncDelete(StreamStateHandle streamStateHandle) {
        if (streamStateHandle != null) {
            LOG.trace("Scheduled delete of state handle {}.", streamStateHandle);
            Runnable discardRunner =
                    () -> {
                        try {
                            streamStateHandle.discardState();
                        } catch (Exception exception) {
                            LOG.warn(
                                    "A problem occurred during asynchronous disposal of a stream handle {}.",
                                    streamStateHandle);
                        }
                    };
            try {
                asyncDisposalExecutor.execute(discardRunner);
            } catch (RejectedExecutionException ex) {
                discardRunner.run();
            }
        }
    }

    @Override
    public void close() throws IOException {
        asyncDisposalExecutor.shutdown();
        handleToLastUsedCheckpointID.clear();
    }
}
