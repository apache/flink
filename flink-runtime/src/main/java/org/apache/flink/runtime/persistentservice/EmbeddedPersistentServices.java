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

package org.apache.flink.runtime.persistentservice;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.StandaloneJobGraphStore;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the {@link PersistentServices} for the non-high-availability case where all
 * participants (ResourceManager, JobManagers, TaskManagers) run in the same process.
 *
 * <p>This implementation has no dependencies on any external services. It stores checkpoints and
 * metadata simply on the heap or on a local file system and therefore in a storage without
 * guarantees.
 */
public class EmbeddedPersistentServices implements PersistentServices {
    private final Object lock = new Object();

    private JobResultStore jobResultStore = new EmbeddedJobResultStore();

    private final VoidBlobStore voidBlobStore = new VoidBlobStore();

    private final JobGraphStore jobGraphStore = new StandaloneJobGraphStore();

    private CheckpointRecoveryFactory checkpointRecoveryFactory;

    private boolean closed = false;

    public EmbeddedPersistentServices() {
        this.checkpointRecoveryFactory = new StandaloneCheckpointRecoveryFactory();
    }

    @VisibleForTesting
    public EmbeddedPersistentServices(CheckpointRecoveryFactory checkpointRecoveryFactory) {
        this.checkpointRecoveryFactory = checkpointRecoveryFactory;
    }

    @VisibleForTesting
    public EmbeddedPersistentServices(JobResultStore jobResultStore) {
        this.jobResultStore = jobResultStore;
        this.checkpointRecoveryFactory = new StandaloneCheckpointRecoveryFactory();
    }

    @Override
    public CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception {
        synchronized (lock) {
            checkNotClose();

            return checkpointRecoveryFactory;
        }
    }

    @Override
    public JobGraphStore getJobGraphStore() throws Exception {
        synchronized (lock) {
            checkNotClose();

            return jobGraphStore;
        }
    }

    @Override
    public JobResultStore getJobResultStore() throws Exception {
        synchronized (lock) {
            checkNotClose();

            return jobResultStore;
        }
    }

    @Override
    public BlobStore getBlobStore() throws IOException {
        synchronized (lock) {
            checkNotClose();

            return voidBlobStore;
        }
    }

    @Override
    public void cleanup() throws Exception {}

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (!closed) {
                closed = true;
            }
        }
    }

    @GuardedBy("lock")
    protected void checkNotClose() {
        checkState(!closed, "embedded persistent services are shut down");
    }
}
