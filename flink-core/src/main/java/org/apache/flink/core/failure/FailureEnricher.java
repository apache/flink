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

package org.apache.flink.core.failure;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Failure Enricher enabling custom logic and attaching metadata in the form of labels to each type
 * of failure as tracked in the JobMaster.
 */
@Experimental
public interface FailureEnricher {

    /**
     * Method to list all the label Keys the enricher can associate with Values in case of a failure
     * {@code processFailure}. Note that Keys must unique and properly defined per enricher
     * implementation otherwise will be ignored.
     *
     * @return the unique label Keys of the FailureEnricher
     */
    Set<String> getOutputKeys();

    /**
     * Method to handle a failure as part of the enricher and optionally return a map of KV pairs
     * (labels). Note that Values should only be associated with Keys from {@code getOutputKeys}
     * method otherwise will be ignored.
     *
     * @param cause the exception that caused this failure
     * @param context the context that includes extra information (e.g., if it was a global failure)
     * @return map of KV pairs (labels) associated with the failure
     */
    CompletableFuture<Map<String, String>> processFailure(
            final Throwable cause, final Context context);

    /**
     * An interface used by the {@link FailureEnricher}. Context includes an executor pool for the
     * enrichers to run heavy operations, the Classloader used for code gen, and other metadata.
     */
    @Experimental
    interface Context {

        /** Type of failure. */
        enum FailureType {
            /**
             * The failure has occurred in the scheduler context and can't be tracked back to a
             * particular task.
             */
            GLOBAL,
            /** The failure has been reported by a particular task. */
            TASK,
            /**
             * The TaskManager has non-gracefully disconnected from the JobMaster or we have not
             * received heartbeats for the {@link
             * org.apache.flink.configuration.HeartbeatManagerOptions#HEARTBEAT_INTERVAL configured
             * timeout}.
             */
            TASK_MANAGER
        }

        /**
         * Get the ID of the job.
         *
         * @return the ID of the job
         */
        JobID getJobId();

        /**
         * Get the name of the job.
         *
         * @return the name of the job
         */
        String getJobName();

        /**
         * Get the metric group of the JobMaster.
         *
         * @return the metric group of the JobMaster
         */
        MetricGroup getMetricGroup();

        /**
         * Return the type of the failure e.g., global failure that happened in the scheduler
         * context.
         *
         * @return FailureType
         */
        FailureType getFailureType();

        /**
         * Get the user {@link ClassLoader} used for code generation, UDF loading and other
         * operations requiring reflections on user code.
         *
         * @return the user ClassLoader
         */
        ClassLoader getUserClassLoader();

        /**
         * Get an Executor pool for the Enrichers to run async operations that can potentially be
         * IO-heavy.
         *
         * @return the Executor pool
         */
        Executor getIOExecutor();
    }
}
