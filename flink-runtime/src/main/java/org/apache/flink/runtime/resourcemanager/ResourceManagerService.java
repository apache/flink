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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.util.AutoCloseableAsync;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** Service that maintains lifecycle of {@link ResourceManager}. */
public interface ResourceManagerService extends AutoCloseableAsync {

    /**
     * Start the service.
     *
     * @throws Exception if the service cannot be started
     */
    void start() throws Exception;

    /**
     * Return termination future of the service.
     *
     * @return termination future of the service.
     */
    CompletableFuture<Void> getTerminationFuture();

    /**
     * Deregister the Flink application from the resource management system by signalling the {@link
     * ResourceManager}.
     *
     * @param applicationStatus to terminate the application with
     * @param diagnostics additional information about the shut down, can be {@code null}
     * @return Future which is completed once the shut down
     */
    CompletableFuture<Void> deregisterApplication(
            final ApplicationStatus applicationStatus, final @Nullable String diagnostics);
}
