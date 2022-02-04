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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;

/**
 * The {@link FailureResult} describes how a failure shall be handled. Currently, there are two
 * alternatives: Either restarting the job or failing it.
 */
class FailureResult {
    @Nullable private final Duration backoffTime;

    private final Throwable failureCause;

    private FailureResult(Throwable failureCause, @Nullable Duration backoffTime) {
        this.backoffTime = backoffTime;
        this.failureCause = failureCause;
    }

    boolean canRestart() {
        return backoffTime != null;
    }

    Duration getBackoffTime() {
        Preconditions.checkState(
                canRestart(), "Failure result must be restartable to return a backoff time.");
        return backoffTime;
    }

    Throwable getFailureCause() {
        return failureCause;
    }

    /**
     * Creates a FailureResult which allows to restart the job.
     *
     * @param failureCause failureCause for restarting the job
     * @param backoffTime backoffTime to wait before restarting the job
     * @return FailureResult which allows to restart the job
     */
    static FailureResult canRestart(Throwable failureCause, Duration backoffTime) {
        return new FailureResult(failureCause, backoffTime);
    }

    /**
     * Creates FailureResult which does not allow to restart the job.
     *
     * @param failureCause failureCause describes the reason why the job cannot be restarted
     * @return FailureResult which does not allow to restart the job
     */
    static FailureResult canNotRestart(Throwable failureCause) {
        return new FailureResult(failureCause, null);
    }
}
