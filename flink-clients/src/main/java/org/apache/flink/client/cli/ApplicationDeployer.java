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

package org.apache.flink.client.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;

/**
 * An interface to be used by the {@link CliFrontend}
 * to submit user programs for execution.
 */
@Internal
public interface ApplicationDeployer {

	/**
	 * Submits a user program for execution and runs the main user method on the cluster.
	 *
	 * @param configuration the configuration containing all the necessary
	 *                        information about submitting the user program.
	 * @param applicationConfiguration an {@link ApplicationConfiguration} specific to
	 *                                   the application to be executed.
	 */
	<ClusterID> void run(
			final Configuration configuration,
			final ApplicationConfiguration applicationConfiguration) throws Exception;
}
