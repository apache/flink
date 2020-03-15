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
import org.apache.flink.util.AutoCloseableAsync;

import java.util.concurrent.CompletableFuture;

/**
 * The {@link DispatcherRunner} encapsulates how a {@link Dispatcher} is being executed.
 */
public interface DispatcherRunner extends AutoCloseableAsync {

	/**
	 * Return shut down future of this runner. The shut down future is being
	 * completed with the final {@link ApplicationStatus} once the runner wants
	 * to shut down.
	 *
	 * @return future with the final application status
	 */
	CompletableFuture<ApplicationStatus> getShutDownFuture();
}
