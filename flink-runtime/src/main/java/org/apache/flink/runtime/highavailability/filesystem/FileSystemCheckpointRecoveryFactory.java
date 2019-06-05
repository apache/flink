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
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.Executor;

/**
 * Checkpoint recovery factory class for file system backend to instantiate
 * {@link CompletedCheckpointStore} and {@link CheckpointIDCounter} implementations (per JobID).
 */

public class FileSystemCheckpointRecoveryFactory  implements CheckpointRecoveryFactory {
    private final Configuration config;
    private final Executor executor;

    /**
     * Factory constructor.
     *
     * 	@param config		    Configuration
     * 	@param executor    		Current executor
     */

    public FileSystemCheckpointRecoveryFactory(Configuration config, Executor executor) {
        this.config = (Configuration) Preconditions.checkNotNull(config, "Configuration");
        this.executor = (Executor)Preconditions.checkNotNull(executor, "Executor");
    }

    /**
     * Create CompletedCheckpointStore.
     *
     * 	@param jobId		                        job id
     * 	@param maxNumberOfCheckpointsToRetain    	max number of checkpoints to remember
     * 	@param userClassLoader    	                Classloader
     */

    public CompletedCheckpointStore createCheckpointStore(JobID jobId, int maxNumberOfCheckpointsToRetain, ClassLoader userClassLoader) throws Exception {
        return FileSystemUtils.createCompletedCheckpoints(this.config, jobId, maxNumberOfCheckpointsToRetain, this.executor);
    }

    /**
     * Create CompletedCheckpointStore.
     *
     * 	@param jobId		                        job id
     */

    public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception {
        return FileSystemUtils.createCheckPointIDCounter(this.config, jobId);
    }
}