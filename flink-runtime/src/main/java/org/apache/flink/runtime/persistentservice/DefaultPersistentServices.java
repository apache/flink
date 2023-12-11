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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.FileSystemJobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * Default persistent services based on distributed system(e.g. Zookeeper, Kubernetes). It will help
 * with creating all the services provide access to highly available storage and registries, as well
 * as distributed counters.
 */
public class DefaultPersistentServices implements PersistentServices {

    private final JobResultStore jobResultStore;

    private final BlobStoreService blobStoreService;

    private final SupplierWithException<CheckpointRecoveryFactory, Exception>
            checkpointRecoveryFactorySupplier;

    private final SupplierWithException<JobGraphStore, Exception> jobGraphStoreSupplier;

    public DefaultPersistentServices(
            Configuration configuration,
            Executor ioExecutor,
            SupplierWithException<CheckpointRecoveryFactory, Exception>
                    checkpointRecoveryFactorySupplier,
            SupplierWithException<JobGraphStore, Exception> jobGraphStoreSupplier)
            throws Exception {
        this.jobResultStore = FileSystemJobResultStore.fromConfiguration(configuration, ioExecutor);
        this.blobStoreService = BlobUtils.createBlobStoreFromConfig(configuration);
        this.checkpointRecoveryFactorySupplier = checkpointRecoveryFactorySupplier;
        this.jobGraphStoreSupplier = jobGraphStoreSupplier;
    }

    @Override
    public CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception {
        return checkpointRecoveryFactorySupplier.get();
    }

    @Override
    public JobGraphStore getJobGraphStore() throws Exception {
        return jobGraphStoreSupplier.get();
    }

    @Override
    public JobResultStore getJobResultStore() throws Exception {
        return jobResultStore;
    }

    @Override
    public BlobStore getBlobStore() throws IOException {
        return blobStoreService;
    }

    @Override
    public void cleanup() throws Exception {
        blobStoreService.cleanupAllData();
    }

    @Override
    public void close() throws Exception {
        blobStoreService.close();
    }
}
