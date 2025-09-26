/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.security.watch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

public interface LocalFSWatchServiceListener {

    enum ReloadState {
        CLEAN, // Context is up to date
        DIRTY, // Context needs reloading
        RELOADING // Context is currently being reloaded
    }

    @FunctionalInterface
    interface ContextLoader {

        void loadContext() throws Exception;
    }

    Logger LOG = LoggerFactory.getLogger(LocalFSWatchServiceListener.class);

    /**
     * Get the current reload state. Implementations should provide their own state management.
     *
     * @return the current reload state
     */
    AtomicReference<ReloadState> getReloadStateReference();

    default void onWatchStarted(Path realDirectoryPath) {}

    default void onFileOrDirectoryCreated(Path relativePath) {}

    default void onFileOrDirectoryDeleted(Path relativePath) {}

    default void onFileOrDirectoryModified(Path relativePath) {
        getReloadStateReference().compareAndSet(ReloadState.CLEAN, ReloadState.DIRTY);
    }

    default boolean reloadContextIfNeeded(ContextLoader loader) {
        AtomicReference<ReloadState> reloadState = getReloadStateReference();
        // Only one thread can transition from DIRTY to RELOADING
        if (reloadState.compareAndSet(ReloadState.DIRTY, ReloadState.RELOADING)) {
            try {
                loader.loadContext();
                // Successfully loaded, mark as clean
                reloadState.set(ReloadState.CLEAN);
                return true;
            } catch (Exception e) {
                LOG.warn("Failed to reload context", e);
                // Failed to load, mark as dirty for retry
                reloadState.set(ReloadState.DIRTY);
            }
        }
        return false;
        // If state is CLEAN, do nothing
        // If state is RELOADING, another thread is handling it, so we can proceed with current
        // context
    }

    /**
     * Abstract base class that provides a default implementation of LocalFSWatchServiceListener
     * with instance-level reload state management.
     */
    abstract class AbstractLocalFSWatchServiceListener implements LocalFSWatchServiceListener {
        private final AtomicReference<ReloadState> reloadState =
                new AtomicReference<>(ReloadState.CLEAN);

        @Override
        public final AtomicReference<ReloadState> getReloadStateReference() {
            return reloadState;
        }
    }
}
