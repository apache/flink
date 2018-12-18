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

package org.apache.flink.yarn;

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Looks up the method {@link RegisterApplicationMasterResponse#getContainersFromPreviousAttempts()}
 * once and saves the method. This saves computation time on subsequent calls.
 */
class RegisterApplicationMasterResponseReflector {

	private final Logger logger;

	/**
	 * Reflected method {@link RegisterApplicationMasterResponse#getContainersFromPreviousAttempts()}.
	 */
	private Method method;

	RegisterApplicationMasterResponseReflector(final Logger log) {
		this(log, RegisterApplicationMasterResponse.class);
	}

	@VisibleForTesting
	RegisterApplicationMasterResponseReflector(final Logger log, final Class<?> clazz) {
		this.logger = requireNonNull(log);
		requireNonNull(clazz);

		try {
			method = clazz.getMethod("getContainersFromPreviousAttempts");
		} catch (NoSuchMethodException e) {
			// that happens in earlier Hadoop versions (pre 2.2)
			logger.info("Cannot reconnect to previously allocated containers. " +
				"This YARN version does not support 'getContainersFromPreviousAttempts()'");
		}
	}

	/**
	 * Checks if a YARN application still has registered containers. If the application master
	 * registered at the ResourceManager for the first time, this list will be empty. If the
	 * application master registered a repeated time (after a failure and recovery), this list
	 * will contain the containers that were previously allocated.
	 *
	 * @param response The response object from the registration at the ResourceManager.
	 * @return A list with containers from previous application attempt.
	 */
	List<Container> getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse response) {
		return getContainersFromPreviousAttemptsUnsafe(response);
	}

	/**
	 * Same as {@link #getContainersFromPreviousAttempts(RegisterApplicationMasterResponse)} but
	 * allows to pass objects that are not of type {@link RegisterApplicationMasterResponse}.
	 */
	@VisibleForTesting
	List<Container> getContainersFromPreviousAttemptsUnsafe(final Object response) {
		if (method != null && response != null) {
			try {
				@SuppressWarnings("unchecked")
				final List<Container> containers = (List<Container>) method.invoke(response);
				if (containers != null && !containers.isEmpty()) {
					return containers;
				}
			} catch (Exception t) {
				logger.error("Error invoking 'getContainersFromPreviousAttempts()'", t);
			}
		}

		return Collections.emptyList();
	}

	@VisibleForTesting
	Method getMethod() {
		return method;
	}
}
