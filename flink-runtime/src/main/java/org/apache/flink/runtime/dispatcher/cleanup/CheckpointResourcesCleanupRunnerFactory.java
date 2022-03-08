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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.state.SharedStateRegistry;

import java.util.concurrent.Executor;

/**
 * {@code CheckpointResourcesCleanupRunnerFactory} implements {@link CleanupRunnerFactory} providing
 * a factory method for creating {@link CheckpointResourcesCleanupRunner} instances.
 */
public enum CheckpointResourcesCleanupRunnerFactory implements CleanupRunnerFactory {
    INSTANCE;

    @Override
    public CheckpointResourcesCleanupRunner create(
            JobResult jobResult,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            Configuration configuration,
            Executor cleanupExecutor) {
        return new CheckpointResourcesCleanupRunner(
                jobResult,
                checkpointRecoveryFactory,
                SharedStateRegistry.DEFAULT_FACTORY,
                configuration,
                cleanupExecutor,
                System.currentTimeMillis());
    }
}
