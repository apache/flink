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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.List;

/**
 * An interface to be implemented by the entities responsible for application
 * submission for the different deployment environments.
 *
 * <p>This interface assumes access to the cluster's {@link DispatcherGateway},
 * and it does not go through the publicly exposed REST API.
 */
@Internal
public interface ApplicationRunner {

	/**
	 * Runs the application using the provided {@code dispatcherGateway}.
	 *
	 * @param dispatcherGateway the dispatcher of the cluster to run the application.
	 * @param program the {@link PackagedProgram} containing the user's main method.
	 * @param configuration the configuration used to run the application.
	 *
	 * @return a list of the submitted jobs that belong to the provided application.
	 */
	List<JobID> run(final DispatcherGateway dispatcherGateway, final PackagedProgram program, final Configuration configuration);
}
