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

/**
 * Listener interface for file system watch events.
 *
 * <p>This interface provides a mechanism for components to respond to file system changes and
 * manage resource reloading (e.g., SSL certificates, configuration files) in a thread-safe manner.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * public class MySslContext extends AbstractLocalFSWatchServiceListener {
 *     private volatile SSLContext sslContext;
 *
 *     public SSLContext getContext() {
 *         reloadContextIfNeeded(this::loadSslContext);
 *         return sslContext;
 *     }
 *
 *     private void loadSslContext() throws Exception {
 *         this.sslContext = SSLContext.getInstance("TLS");
 *         // ... initialize from files
 *     }
 * }
 * }</pre>
 */
public interface LocalFSWatchServiceListener {

    /**
     * Represents the current state of the resource reload lifecycle.
     *
     * <p>The state transitions are:
     *
     * <ul>
     *   <li>CLEAN → DIRTY: When a watched file is modified
     *   <li>DIRTY → RELOADING: When a thread begins reloading (atomic CAS operation)
     *   <li>RELOADING → CLEAN: When reload completes successfully
     *   <li>RELOADING → DIRTY: When reload fails (allows retry)
     * </ul>
     */
    enum ReloadState {
        /** Resources are up to date and no reload is needed. */
        CLEAN,
        /** Resources need reloading due to file system changes. */
        DIRTY,
        /** A thread is currently reloading resources. */
        RELOADING
    }

    /**
     * Functional interface for loading or reloading context/resources.
     *
     * <p>Implementations should load resources (e.g., SSL certificates, configuration) from the
     * file system. This is called by {@link #reloadContextIfNeeded(ContextLoader)} when resources
     * are marked as {@link ReloadState#DIRTY}.
     */
    @FunctionalInterface
    interface ContextLoader {

        /**
         * Loads or reloads the context/resources.
         *
         * @throws Exception if the context cannot be loaded
         */
        void loadContext() throws Exception;
    }

    Logger LOG = LoggerFactory.getLogger(LocalFSWatchServiceListener.class);

    /**
     * Returns the atomic reference to the reload state for this listener.
     *
     * <p>Implementations should provide their own state management, typically through an {@link
     * AtomicReference} field.
     *
     * @return the atomic reference to the current reload state
     */
    AtomicReference<ReloadState> getReloadStateReference();

    /**
     * Called when the file system watch service starts monitoring a directory.
     *
     * <p>This is a lifecycle callback invoked once when the watch service successfully registers a
     * directory for monitoring. The default implementation does nothing.
     *
     * @param realDirectoryPath the absolute path to the directory being watched
     */
    default void onWatchStarted(Path realDirectoryPath) {}

    /**
     * Called when a file or directory is created in a watched directory.
     *
     * <p>The default implementation marks the reload state as {@link ReloadState#DIRTY}, indicating
     * that resources need reloading. This handles the case where certificates are replaced by
     * deleting and recreating files. Override this method to customize creation handling.
     *
     * @param relativePath the relative path of the created file or directory
     */
    default void onFileOrDirectoryCreated(Path relativePath) {
        getReloadStateReference().compareAndSet(ReloadState.CLEAN, ReloadState.DIRTY);
        LOG.debug(
                "File {} has been created in {}, reloadState={}",
                relativePath,
                this,
                getReloadStateReference().get());
    }

    /**
     * Called when a file or directory is deleted from a watched directory.
     *
     * <p>The default implementation does nothing. Override this method to handle deletion events.
     *
     * @param relativePath the relative path of the deleted file or directory
     */
    default void onFileOrDirectoryDeleted(Path relativePath) {}

    /**
     * Called when a file or directory is modified in a watched directory.
     *
     * <p>The default implementation marks the reload state as {@link ReloadState#DIRTY}, indicating
     * that resources need reloading. Override this method to customize modification handling.
     *
     * @param relativePath the relative path of the modified file or directory
     */
    default void onFileOrDirectoryModified(Path relativePath) {
        getReloadStateReference().compareAndSet(ReloadState.CLEAN, ReloadState.DIRTY);
        LOG.debug(
                "File {} has been modified in {}, reloadState={}",
                relativePath,
                this,
                getReloadStateReference().get());
    }

    /**
     * Reloads the context if needed based on the current reload state.
     *
     * <p>This method implements thread-safe context reloading using atomic compare-and-set
     * operations:
     *
     * <ul>
     *   <li>If state is {@link ReloadState#DIRTY}: Attempts to transition to {@link
     *       ReloadState#RELOADING} and invoke the loader. Only one thread succeeds in this
     *       transition.
     *   <li>If state is {@link ReloadState#CLEAN}: Returns immediately without reloading (resources
     *       are up to date).
     *   <li>If state is {@link ReloadState#RELOADING}: Returns immediately (another thread is
     *       handling the reload).
     * </ul>
     *
     * <p>On successful reload, the state transitions to {@link ReloadState#CLEAN}. On failure, the
     * state returns to {@link ReloadState#DIRTY} to allow retry.
     *
     * @param loader the context loader to invoke if reloading is needed
     * @return {@code true} if this invocation successfully reloaded the context, {@code false}
     *     otherwise
     */
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
