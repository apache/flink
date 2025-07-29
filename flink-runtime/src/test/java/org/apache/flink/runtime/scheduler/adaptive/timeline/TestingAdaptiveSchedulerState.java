/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.scheduler.adaptive.State;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Testing helper for adaptive scheduler state span. */
public class TestingAdaptiveSchedulerState implements State {

    private final Durable durable;

    TestingAdaptiveSchedulerState(Long inTimestamp, @Nullable Long outTimestamp) {
        this.durable = new Durable(inTimestamp, outTimestamp);
    }

    @Override
    public Durable getDurable() {
        return durable;
    }

    @Override
    public void cancel() {}

    @Override
    public void suspend(Throwable cause) {}

    @Override
    public JobID getJobId() {
        return null;
    }

    @Override
    public JobStatus getJobStatus() {
        return null;
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return null;
    }

    @Override
    public Logger getLogger() {
        return null;
    }

    @Override
    public void handleGlobalFailure(
            Throwable cause, CompletableFuture<Map<String, String>> failureLabels) {}
}
