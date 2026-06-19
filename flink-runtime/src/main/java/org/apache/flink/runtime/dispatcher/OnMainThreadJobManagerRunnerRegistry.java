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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.util.WrappingProxy;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@code OnMainThreadJobManagerRunnerRegistry} implements {@link JobManagerRunnerRegistry} guarding
 * the passed {@code JobManagerRunnerRegistry} instance in a way that it only allows modifying
 * methods to be executed on the component's main thread.
 *
 * @see ComponentMainThreadExecutor
 */
public class OnMainThreadJobManagerRunnerRegistry
        implements JobManagerRunnerRegistry, WrappingProxy<JobManagerRunnerRegistry> {

    private final JobManagerRunnerRegistry delegate;
    private final ComponentMainThreadExecutor mainThreadExecutor;

    public OnMainThreadJobManagerRunnerRegistry(
            JobManagerRunnerRegistry delegate, ComponentMainThreadExecutor mainThreadExecutor) {
        this.delegate = delegate;
        this.mainThreadExecutor = mainThreadExecutor;
    }

    @Override
    public boolean isRegistered(JobID jobId) {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.isRegistered(jobId);
    }

    @Override
    public void register(JobManagerRunner jobManagerRunner) {
        mainThreadExecutor.assertRunningInMainThread();
        delegate.register(jobManagerRunner);
    }

    @Override
    public JobManagerRunner get(JobID jobId) {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.get(jobId);
    }

    @Override
    public int size() {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.size();
    }

    @Override
    public Set<JobID> getRunningJobIds() {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.getRunningJobIds();
    }

    @Override
    public Collection<JobManagerRunner> getJobManagerRunners() {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.getJobManagerRunners();
    }

    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.localCleanupAsync(jobId, executor);
    }

    @Override
    public JobManagerRunner unregister(JobID jobId) {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.unregister(jobId);
    }

    /**
     * Returns the delegated {@link JobManagerRunnerRegistry}. This method can be used to workaround
     * the main thread safeguard.
     */
    @Override
    public JobManagerRunnerRegistry getWrappedDelegate() {
        return this.delegate;
    }
}
