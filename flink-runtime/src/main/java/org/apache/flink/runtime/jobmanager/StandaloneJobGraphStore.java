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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;

import java.util.Collection;
import java.util.Collections;

/**
 * {@link JobGraph} instances for JobManagers running in {@link HighAvailabilityMode#NONE}.
 *
 * <p>All operations are NoOps, because {@link JobGraph} instances cannot be recovered in this
 * recovery mode.
 */
public class StandaloneJobGraphStore implements JobGraphStore {

    @Override
    public void start(JobGraphListener jobGraphListener) throws Exception {
        // Nothing to do
    }

    @Override
    public void stop() {
        // Nothing to do
    }

    @Override
    public void putJobGraph(JobGraph jobGraph) {
        // Nothing to do
    }

    @Override
    public void putJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) {
        // Nothing to do
    }

    @Override
    public Collection<JobID> getJobIds() {
        return Collections.emptyList();
    }

    @Override
    public JobGraph recoverJobGraph(JobID jobId) {
        return null;
    }
}
