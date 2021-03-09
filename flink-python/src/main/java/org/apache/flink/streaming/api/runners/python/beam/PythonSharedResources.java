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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.python.env.PythonEnvironmentManager;

import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;

import java.util.ArrayList;
import java.util.List;

/** The set of resources that can be shared by all the Python operators in a slot. */
@Internal
public final class PythonSharedResources implements AutoCloseable {

    /**
     * The bundle factory which has all job-scoped information and can be used to create a {@link
     * StageBundleFactory}.
     */
    private final JobBundleFactory jobBundleFactory;

    /** An environment for executing Python UDFs. */
    private final Environment environment;

    /** Keep track of the PythonEnvironmentManagers of the Python operators in one slot. */
    private final List<PythonEnvironmentManager> environmentManagers;

    /** Keep track of the number of Python operators sharing this Python resource. */
    private int refCnt;

    PythonSharedResources(JobBundleFactory jobBundleFactory, Environment environment) {
        this.jobBundleFactory = jobBundleFactory;
        this.environment = environment;
        this.environmentManagers = new ArrayList<>();
        this.refCnt = 0;
    }

    JobBundleFactory getJobBundleFactory() {
        return jobBundleFactory;
    }

    Environment getEnvironment() {
        return environment;
    }

    void addPythonEnvironmentManager(PythonEnvironmentManager environmentManager) {
        environmentManagers.add(environmentManager);
        refCnt++;
    }

    /**
     * Release a Python operator which shares this Python resource. Returns true if there are no
     * more Python operators sharing this Python resource.
     */
    boolean release() {
        refCnt--;
        return refCnt == 0;
    }

    @Override
    public void close() throws Exception {
        jobBundleFactory.close();
        for (PythonEnvironmentManager environmentManager : environmentManagers) {
            environmentManager.close();
        }
    }
}
