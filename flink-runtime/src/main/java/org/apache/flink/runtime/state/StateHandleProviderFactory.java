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

package org.apache.flink.runtime.state;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;

import java.io.Serializable;

/**
 * State handler provider factory.
 *
 * <p>This is going to be superseded soon.
 */
public class StateHandleProviderFactory {

	/**
	 * Creates a {@link org.apache.flink.runtime.state.FileStateHandle.FileStateHandleProvider} at
	 * the configured recovery path.
	 */
	public static <T extends Serializable> StateHandleProvider<T> createRecoveryFileStateHandleProvider(
			Configuration config) {

		StateBackend stateBackend = StateBackend.fromConfig(config);

		if (stateBackend == StateBackend.FILESYSTEM) {
			String recoveryPath = config.getString(
					ConfigConstants.STATE_BACKEND_FS_RECOVERY_PATH, "");

			if (recoveryPath.equals("")) {
				throw new IllegalConfigurationException("Missing recovery path. Specify via " +
						"configuration key '" + ConfigConstants.STATE_BACKEND_FS_RECOVERY_PATH + "'.");
			}
			else {
				return FileStateHandle.createProvider(recoveryPath);
			}
		}
		else {
			throw new IllegalConfigurationException("Unexpected state backend configuration " +
					stateBackend);
		}
	}

}
