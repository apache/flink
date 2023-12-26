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

package org.apache.flink.runtime.highavailability.nonha;

import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.StandaloneJobGraphStore;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Abstract base class for non high-availability services.
 *
 * <p>This class returns the standalone variants for the checkpoint recovery factory, the submitted
 * job graph store, the running jobs registry and the blob store.
 */
public abstract class AbstractNonHaServices implements HighAvailabilityServices {
    protected final Object lock = new Object();

    private final JobResultStore jobResultStore;

    private final VoidBlobStore voidBlobStore;

    private boolean shutdown;

    public AbstractNonHaServices() {
        this.jobResultStore = new EmbeddedJobResultStore();
        this.voidBlobStore = new VoidBlobStore();

        shutdown = false;
    }

    // ----------------------------------------------------------------------
    // HighAvailabilityServices method implementations
    // ----------------------------------------------------------------------

    @Override
    public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
        synchronized (lock) {
            checkNotShutdown();

            return new StandaloneCheckpointRecoveryFactory();
        }
    }

    @Override
    public JobGraphStore getJobGraphStore() throws Exception {
        synchronized (lock) {
            checkNotShutdown();

            return new StandaloneJobGraphStore();
        }
    }

    @Override
    public JobResultStore getJobResultStore() throws Exception {
        synchronized (lock) {
            checkNotShutdown();

            return jobResultStore;
        }
    }

    @Override
    public BlobStore createBlobStore() throws IOException {
        synchronized (lock) {
            checkNotShutdown();

            return voidBlobStore;
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;
            }
        }
    }

    @Override
    public void cleanupAllData() throws Exception {
        // this stores no data, do nothing here
    }

    // ----------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------

    @GuardedBy("lock")
    protected void checkNotShutdown() {
        checkState(!shutdown, "high availability services are shut down");
    }

    protected boolean isShutDown() {
        return shutdown;
    }
}
