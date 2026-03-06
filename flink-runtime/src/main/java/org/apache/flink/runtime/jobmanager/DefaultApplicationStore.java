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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.persistence.ResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default implementation for {@link ApplicationStore}. Combined with different {@link
 * StateHandleStore}, we could persist the applications to various distributed storage.
 */
public class DefaultApplicationStore<R extends ResourceVersion<R>> implements ApplicationStore {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultApplicationStore.class);

    private final Object lock = new Object();

    /** The set of IDs of all added applications. */
    @GuardedBy("lock")
    private final Set<ApplicationID> addedApplications = new HashSet<>();

    /** Submitted applications handle store. */
    private final StateHandleStore<ApplicationStoreEntry, R> applicationStateHandleStore;

    private final ApplicationStoreUtil applicationStoreUtil;

    /** Flag indicating whether this instance is running. */
    @GuardedBy("lock")
    private volatile boolean running;

    public DefaultApplicationStore(
            StateHandleStore<ApplicationStoreEntry, R> stateHandleStore,
            ApplicationStoreUtil applicationStoreUtil) {
        this.applicationStateHandleStore = checkNotNull(stateHandleStore);
        this.applicationStoreUtil = checkNotNull(applicationStoreUtil);

        this.running = false;
    }

    @Override
    public void start() throws Exception {
        synchronized (lock) {
            if (!running) {
                running = true;
            }
        }
    }

    @Override
    public void stop() throws Exception {
        synchronized (lock) {
            if (running) {
                running = false;
                LOG.info("Stopping DefaultApplicationStore.");
                Exception exception = null;

                try {
                    applicationStateHandleStore.releaseAll();
                } catch (Exception e) {
                    exception = e;
                }

                if (exception != null) {
                    throw new FlinkException(
                            "Could not properly stop the DefaultApplicationStore.", exception);
                }
            }
        }
    }

    @Override
    public Optional<ApplicationStoreEntry> recoverApplication(ApplicationID applicationId)
            throws Exception {
        checkNotNull(applicationId, "Application ID");

        LOG.debug("Recovering application {} from {}.", applicationId, applicationStateHandleStore);

        final String name = applicationStoreUtil.applicationIdToName(applicationId);

        synchronized (lock) {
            verifyIsRunning();

            boolean success = false;

            RetrievableStateHandle<ApplicationStoreEntry> applicationRetrievableStateHandle;

            try {
                try {
                    applicationRetrievableStateHandle =
                            applicationStateHandleStore.getAndLock(name);
                } catch (StateHandleStore.NotExistException ignored) {
                    success = true;
                    return Optional.empty();
                } catch (Exception e) {
                    throw new FlinkException(
                            "Could not retrieve the submitted application state handle "
                                    + "for "
                                    + name
                                    + " from the submitted application store.",
                            e);
                }

                ApplicationStoreEntry application;
                try {
                    application = applicationRetrievableStateHandle.retrieveState();
                } catch (ClassNotFoundException cnfe) {
                    throw new FlinkException(
                            "Could not retrieve submitted application from state handle under "
                                    + name
                                    + ". This indicates that you are trying to recover from state written by an "
                                    + "older Flink version which is not compatible. Try cleaning the state handle store.",
                            cnfe);
                } catch (IOException ioe) {
                    throw new FlinkException(
                            "Could not retrieve submitted application from state handle under "
                                    + name
                                    + ". This indicates that the retrieved state handle is broken. Try cleaning the state handle "
                                    + "store.",
                            ioe);
                }

                addedApplications.add(applicationId);

                LOG.info("Recovered {} ({}).", application.getName(), applicationId);

                success = true;
                return Optional.of(application);
            } finally {
                if (!success) {
                    applicationStateHandleStore.release(name);
                }
            }
        }
    }

    @Override
    public void putApplication(ApplicationStoreEntry application) throws Exception {
        checkNotNull(application, "Application");

        final ApplicationID applicationID = application.getApplicationId();
        final String name = applicationStoreUtil.applicationIdToName(applicationID);

        LOG.debug("Adding application {} to {}.", applicationID, applicationStateHandleStore);

        boolean success = false;

        while (!success) {
            synchronized (lock) {
                verifyIsRunning();

                final R currentVersion = applicationStateHandleStore.exists(name);

                if (!currentVersion.isExisting()) {
                    try {
                        applicationStateHandleStore.addAndLock(name, application);

                        addedApplications.add(applicationID);

                        success = true;
                    } catch (StateHandleStore.AlreadyExistException ignored) {
                        LOG.warn(
                                "{} already exists in {}.",
                                application,
                                applicationStateHandleStore);
                    }
                } else if (addedApplications.contains(applicationID)) {
                    try {
                        applicationStateHandleStore.replace(name, currentVersion, application);
                        LOG.info("Updated {} in {}.", application, getClass().getSimpleName());

                        success = true;
                    } catch (StateHandleStore.NotExistException ignored) {
                        LOG.warn(
                                "{} does not exists in {}.",
                                application,
                                applicationStateHandleStore);
                    }
                } else {
                    throw new IllegalStateException(
                            "Trying to update an application you didn't "
                                    + "#getAllSubmittedApplications() or #putApplication() yourself before.");
                }
            }
        }

        LOG.info("Added {} to {}.", application, applicationStateHandleStore);
    }

    @Override
    public CompletableFuture<Void> globalCleanupAsync(
            ApplicationID applicationId, Executor executor) {
        checkNotNull(applicationId, "Application ID");

        return runAsyncWithLockAssertRunning(
                () -> {
                    LOG.debug(
                            "Removing application {} from {}.",
                            applicationId,
                            applicationStateHandleStore);

                    final String name = applicationStoreUtil.applicationIdToName(applicationId);
                    releaseAndRemoveOrThrowCompletionException(applicationId, name);

                    addedApplications.remove(applicationId);

                    LOG.info(
                            "Removed application {} from {}.",
                            applicationId,
                            applicationStateHandleStore);
                },
                executor);
    }

    @GuardedBy("lock")
    private void releaseAndRemoveOrThrowCompletionException(
            ApplicationID applicationId, String applicationName) {
        boolean success;
        try {
            success = applicationStateHandleStore.releaseAndTryRemove(applicationName);
        } catch (Exception e) {
            throw new CompletionException(e);
        }

        if (!success) {
            throw new CompletionException(
                    new FlinkException(
                            String.format(
                                    "Could not remove application with application id %s from %s.",
                                    applicationId, applicationStateHandleStore)));
        }
    }

    private CompletableFuture<Void> runAsyncWithLockAssertRunning(
            ThrowingRunnable<Exception> runnable, Executor executor) {
        return CompletableFuture.runAsync(
                () -> {
                    synchronized (lock) {
                        verifyIsRunning();
                        try {
                            runnable.run();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }
                },
                executor);
    }

    @Override
    public Collection<ApplicationID> getApplicationIds() throws Exception {
        LOG.debug("Retrieving all stored application ids from {}.", applicationStateHandleStore);

        final Collection<String> names;
        try {
            names = applicationStateHandleStore.getAllHandles();
        } catch (Exception e) {
            throw new Exception(
                    "Failed to retrieve all application ids from "
                            + applicationStateHandleStore
                            + ".",
                    e);
        }

        final List<ApplicationID> applicationIds = new ArrayList<>(names.size());

        for (String name : names) {
            try {
                applicationIds.add(applicationStoreUtil.nameToApplicationId(name));
            } catch (Exception exception) {
                LOG.warn(
                        "Could not parse application id from {}. This indicates a malformed name.",
                        name,
                        exception);
            }
        }

        LOG.info(
                "Retrieved application ids {} from {}",
                applicationIds,
                applicationStateHandleStore);

        return applicationIds;
    }

    /** Verifies that the state is running. */
    private void verifyIsRunning() {
        checkState(running, "Not running. Forgot to call start()?");
    }
}
