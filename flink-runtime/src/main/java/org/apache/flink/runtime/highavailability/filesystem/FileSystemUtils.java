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

package org.apache.flink.runtime.highavailability.filesystem;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.jobmanager.FileSystemCheckpointIDCounter;
import org.apache.flink.runtime.jobmanager.FileSystemCompletedCheckpointStore;
import org.apache.flink.runtime.jobmanager.FileSystemSubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * Class containing helper functions to support filesystem based HA.
 */

public class FileSystemUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtils.class);

    /**
     * Creates a {@link FileSystemStorageHelper} instance.
     *
     * @param configuration {@link Configuration} object
     * @param prefix Prefix for the created files
     * @param <T> Type of the state objects
     * @return {@link FileSystemStorageHelper} instance
     * @throws IOException if file system state storage cannot be created
     */
    private static <T extends Serializable> FileSystemStorageHelper<T> createFileSystemStateStorage(
            Configuration configuration,
            String prefix) throws IOException {

        String storagePath = configuration.getValue(HighAvailabilityOptions.HA_STORAGE_PATH) + "/ha";
        if(storagePath.startsWith("file://")) {
            storagePath = storagePath.substring(7);
        }
        LOG.info("Creating FileSystemStorageHelper with path " + storagePath + " prefix " + prefix);
        if (isNullOrWhitespaceOnly(storagePath)) {
            throw new IllegalConfigurationException("Configuration is missing the mandatory parameter: " +
                    HighAvailabilityOptions.HA_STORAGE_PATH);
        }

        return new FileSystemStorageHelper<T>(storagePath, prefix);
    }

    /**
     * Creates a {@link SubmittedJobGraphStore} instance.
     *
     * @param configuration {@link Configuration} object
     * @return {@link SubmittedJobGraphStore} instance
     * @throws Exception if the submitted job graph store cannot be created
     */

    public static SubmittedJobGraphStore createSubmittedJobGraphs(
            Configuration configuration) throws Exception {

        checkNotNull(configuration, "Configuration");
        LOG.info("Creating SubmittedJobGraphStore");

        FileSystemStorageHelper<SubmittedJobGraph> stateStorage = createFileSystemStateStorage(configuration, "submittedJobGraph");

        return new FileSystemSubmittedJobGraphStore(stateStorage);
    }

    /**
     * Creates a {@link CompletedCheckpointStore} instance.
     *
     * @param configuration                  {@link Configuration} object
     * @param jobId                          ID of job to create the instance for
     * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain
     * @param executor to run ZooKeeper callbacks
     * @return {@link CompletedCheckpointStore} instance
     * @throws Exception if the completed checkpoint store cannot be created
     */

    public static CompletedCheckpointStore createCompletedCheckpoints(
            Configuration configuration,
            JobID jobId,
            int maxNumberOfCheckpointsToRetain,
            Executor executor) throws Exception {

        checkNotNull(configuration, "Configuration");

        String storagePath = configuration.getValue(HighAvailabilityOptions.HA_STORAGE_PATH);
        if (isNullOrWhitespaceOnly(storagePath)) {
            throw new IllegalConfigurationException("Configuration is missing the mandatory parameter: " +
                    HighAvailabilityOptions.HA_STORAGE_PATH);
        }

        String hexJobId = jobId.toHexString();
        LOG.info("Creating CompletedCheckpointStore for job " + hexJobId);
        FileSystemStorageHelper<CompletedCheckpoint> stateStorage = createFileSystemStateStorage(
                configuration,
                "completedCheckpoint/" + hexJobId);

        final FileSystemCompletedCheckpointStore completedCheckpointStore = new FileSystemCompletedCheckpointStore(
                maxNumberOfCheckpointsToRetain,
                stateStorage,
                executor);

        return completedCheckpointStore;
    }

    /**
     * Creates a {@link CheckpointIDCounter} instance.
     *
     * @param configuration                  {@link Configuration} object
     * @return {@link CheckpointIDCounter}   instance
     * @throws Exception if the completed checkpoint store cannot be created
     */

    public static CheckpointIDCounter createCheckPointIDCounter(
            Configuration configuration, JobID jobId) throws Exception {

        checkNotNull(configuration, "Configuration");

        String hexJobId = jobId.toHexString();
        FileSystemStorageHelper<Integer> counterStorage = createFileSystemStateStorage(
                configuration,
                "checkpointcounter/" + hexJobId);

        LOG.info("Creating CheckpointIDCounter for job " + hexJobId);
        return new FileSystemCheckpointIDCounter(counterStorage);
    }
}
