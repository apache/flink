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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** Implementation of the {@link JobMasterService} for testing purposes. */
public class TestingJobMasterService implements JobMasterService {

    @Nonnull private final String address;

    private final JobMasterGateway jobMasterGateway;

    private final CompletableFuture<Void> terminationFuture;

    private final boolean completeTerminationFutureOnCloseAsync;

    public TestingJobMasterService(
            @Nonnull String address, @Nullable CompletableFuture<Void> terminationFuture) {
        this.address = address;

        jobMasterGateway = new TestingJobMasterGatewayBuilder().build();

        if (terminationFuture == null) {
            this.terminationFuture = new CompletableFuture<>();
            this.completeTerminationFutureOnCloseAsync = true;
        } else {
            this.terminationFuture = terminationFuture;
            this.completeTerminationFutureOnCloseAsync = false;
        }
    }

    public TestingJobMasterService() {
        this("localhost", null);
    }

    @Override
    public JobMasterGateway getGateway() {
        Preconditions.checkNotNull(
                jobMasterGateway, "TestingJobMasterService has not been started yet.");
        return jobMasterGateway;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (completeTerminationFutureOnCloseAsync) {
            terminationFuture.complete(null);
        }

        return terminationFuture;
    }
}
