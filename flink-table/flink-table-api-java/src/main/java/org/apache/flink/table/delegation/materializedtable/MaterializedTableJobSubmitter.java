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

package org.apache.flink.table.delegation.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

/**
 * Provides the caller-specific refresh-job submission and scheduling capabilities a {@link
 * MaterializedTableExecutor} needs to run materialized table operations.
 */
@Internal
public interface MaterializedTableJobSubmitter {

    /**
     * Resolves the target of a refresh job and submits it there with the given execution
     * configuration and INSERT statement.
     *
     * @param executionConfig the configuration used to submit the refresh job.
     * @param insertStatement the INSERT statement executed to refresh the materialized table.
     * @return the result identifying the submitted refresh job and the target to record for it.
     */
    RefreshJobResult submitRefreshJob(Configuration executionConfig, String insertStatement);

    /**
     * Whether refresh jobs can be deployed to application targets, each in its own cluster.
     *
     * <p>Returning {@code true} also switches the executor to the SQL Gateway's behaviour: it skips
     * the target check before any catalog change and the check that a restart stays on the recorded
     * target and cluster.
     */
    boolean supportsApplicationTargets();

    /**
     * Returns the periodic-refresh scheduling capability for full-mode materialized tables, or
     * {@link Optional#empty()} when this job submitter cannot schedule refresh workflows.
     */
    Optional<RefreshWorkflowContext> getRefreshWorkflowContext();
}
