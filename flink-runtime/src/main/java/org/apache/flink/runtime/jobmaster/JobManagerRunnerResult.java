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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

/** The result of the {@link JobManagerRunner}. */
public final class JobManagerRunnerResult {

    @Nullable private final ExecutionGraphInfo executionGraphInfo;

    @Nullable private final Throwable failure;

    private JobManagerRunnerResult(
            @Nullable ExecutionGraphInfo executionGraphInfo, @Nullable Throwable failure) {
        this.executionGraphInfo = executionGraphInfo;
        this.failure = failure;
    }

    public boolean isSuccess() {
        return executionGraphInfo != null && failure == null;
    }

    public boolean isJobNotFinished() {
        return executionGraphInfo == null && failure == null;
    }

    public boolean isInitializationFailure() {
        return executionGraphInfo == null && failure != null;
    }

    /**
     * This method returns the payload of the successful JobManagerRunnerResult.
     *
     * @return the @link ExecutionGraphInfo} of a successfully finished job
     * @throws IllegalStateException if the result is not a success
     */
    public ExecutionGraphInfo getExecutionGraphInfo() {
        Preconditions.checkState(isSuccess());
        return executionGraphInfo;
    }

    /**
     * This method returns the initialization failure.
     *
     * @return the initialization failure
     * @throws IllegalStateException if the result is not an initialization failure
     */
    public Throwable getInitializationFailure() {
        Preconditions.checkState(isInitializationFailure());
        return failure;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobManagerRunnerResult that = (JobManagerRunnerResult) o;
        return Objects.equals(executionGraphInfo, that.executionGraphInfo)
                && Objects.equals(failure, that.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionGraphInfo, failure);
    }

    public static JobManagerRunnerResult forJobNotFinished() {
        return new JobManagerRunnerResult(null, null);
    }

    public static JobManagerRunnerResult forSuccess(ExecutionGraphInfo executionGraphInfo) {
        return new JobManagerRunnerResult(executionGraphInfo, null);
    }

    public static JobManagerRunnerResult forInitializationFailure(Throwable failure) {
        return new JobManagerRunnerResult(null, failure);
    }
}
