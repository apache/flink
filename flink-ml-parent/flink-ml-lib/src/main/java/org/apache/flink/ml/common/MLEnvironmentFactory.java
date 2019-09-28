/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common;

import java.util.HashMap;

/**
 * Factory to get the MLEnvironment using a MLEnvironmentId.
 *
 * <p>The following code snippet shows how to interact with MLEnvironmentFactory.
 * <pre>
 * {@code
 * long mlEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();
 * MLEnvironment mlEnv = MLEnvironmentFactory.get(mlEnvId);
 * }
 * </pre>
 */
public class MLEnvironmentFactory {

	/**
	 * The default MLEnvironmentId.
	 */
	public static final Long DEFAULT_ML_ENVIRONMENT_ID = 0L;

	/**
	 * A 'id' is a unique identifier of a MLEnvironment.
	 */
	private static Long id = 1L;

	/**
	 * Map that hold the MLEnvironment and use the MLEnvironmentId as its key.
	 */
	private static HashMap<Long, MLEnvironment> map = new HashMap<>();

	static {
		map.put(DEFAULT_ML_ENVIRONMENT_ID, new MLEnvironment());
	}

	/**
	 * Get the MLEnvironment using a MLEnvironmentId.
	 *
	 * @param mlEnvId the MLEnvironmentId
	 * @return the MLEnvironment
	 */
	public static synchronized MLEnvironment get(Long mlEnvId) {
		if (!map.containsKey(mlEnvId)) {
			throw new RuntimeException("There is no Environment in factory. " +
				"Maybe you could call `getNewMLEnvironmentId` to create a new MLEnvironmentId");
		}

		return map.get(mlEnvId);
	}

	/**
	 * Get the MLEnvironment use the default MLEnvironmentId.
	 *
	 * @return the default MLEnvironment.
	 */
	public static synchronized MLEnvironment getDefault() {
		return get(DEFAULT_ML_ENVIRONMENT_ID);
	}

	/**
	 * Create a unique MLEnvironment id and set a new MLEnvironment in the factory.
	 *
	 * @return the MLEnvironment id.
	 */
	public static synchronized Long getNewMLEnvironmentId() {
		map.put(id, new MLEnvironment());
		return id++;
	}

	/**
	 * Remove the MLEnvironment using the MLEnvironmentId.
	 *
	 * @param mlEnvId the id.
	 * @return the removed MLEnvironment
	 */
	public static synchronized MLEnvironment remove(Long mlEnvId) {
		return map.remove(mlEnvId);
	}
}
