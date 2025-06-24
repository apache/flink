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

import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.scheduler.GlobalFailureHandler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * An interface for handling and associating global failures with labels from {@link
 * FailureEnricher}. In context of a scheduler we distinguish between local and global failures.
 * Global failure is the one that happens in context of the scheduler (in the JobManager process)
 * and local failure is one that is "local" to an executing task.
 */
public interface LabeledGlobalFailureHandler {

    /**
     * An adapted version of {@link GlobalFailureHandler} that handles and associates global
     * failures with enricher labels.
     *
     * @param cause A cause that describes the global failure.
     * @param failureLabels Labels providing an additional context about the failure.
     */
    void handleGlobalFailure(Throwable cause, CompletableFuture<Map<String, String>> failureLabels);
}
