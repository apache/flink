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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.util.AutoCloseableAsync;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Leader process which encapsulates the lifecycle of the {@link Dispatcher} component. */
interface DispatcherLeaderProcess extends AutoCloseableAsync {

    void start();

    UUID getLeaderSessionId();

    CompletableFuture<DispatcherGateway> getDispatcherGateway();

    CompletableFuture<String> getLeaderAddressFuture();

    CompletableFuture<ApplicationStatus> getShutDownFuture();
}
