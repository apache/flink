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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.UnsupportedOperationExecutor;

import java.util.concurrent.Executor;

/**
 * Factory interface for {@link HighAvailabilityServices}.
 */
public interface HighAvailabilityServicesFactory {

	/**
	 * Creates a {@link HighAvailabilityServices} instance.
	 *
	 * @param configuration Flink configuration
	 * @param executor background task executor
	 * @return instance of {@link HighAvailabilityServices}
	 * @throws Exception when HAServices cannot be created
	 */
	HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) throws Exception;

	/**
	 * Create a {@link ClientHighAvailabilityServices} instance.
	 *
	 * @param configuration Flink configuration
	 * @return instance of {@link ClientHighAvailabilityServices}
	 * @throws Exception when ClientHAServices cannot be created
	 */
	default ClientHighAvailabilityServices createClientHAServices(Configuration configuration) throws Exception {
		return createHAServices(configuration, UnsupportedOperationExecutor.INSTANCE);
	}
}
