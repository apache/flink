/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobMasterService;
import org.apache.flink.runtime.jobmaster.TestingJobMasterService;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Testing implementation of the {@link JobMasterServiceFactory}. */
public class TestingJobMasterServiceFactory implements JobMasterServiceFactory {
    private final Function<OnCompletionActions, CompletableFuture<JobMasterService>>
            jobMasterServiceFunction;

    public TestingJobMasterServiceFactory(
            Function<OnCompletionActions, CompletableFuture<JobMasterService>>
                    jobMasterServiceFunction) {
        this.jobMasterServiceFunction = jobMasterServiceFunction;
    }

    public TestingJobMasterServiceFactory() {
        this(ignored -> CompletableFuture.completedFuture(new TestingJobMasterService()));
    }

    @Override
    public CompletableFuture<JobMasterService> createJobMasterService(
            UUID leaderSessionId, OnCompletionActions onCompletionActions) {
        return jobMasterServiceFunction.apply(onCompletionActions);
    }
}
