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

package org.apache.flink.fs.azurefs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.KeyProvider;
import org.apache.hadoop.fs.azure.KeyProviderException;

/**
 * An implementation of {@link org.apache.hadoop.fs.azure.KeyProvider}, which reads the Azure
 * storage key from an environment variable named "AZURE_STORAGE_KEY".
 *
 */
public class EnvironmentVariableKeyProvider implements KeyProvider {

	public static final String AZURE_STORAGE_KEY_ENV_VARIABLE = "AZURE_STORAGE_KEY";

	@Override
	public String getStorageAccountKey(
			final String s,
			final Configuration configuration) throws KeyProviderException {

		String azureStorageKey = System.getenv(AZURE_STORAGE_KEY_ENV_VARIABLE);

		if (azureStorageKey != null) {
			return azureStorageKey;
		} else {
			throw new KeyProviderException("Unable to retrieve Azure storage key from environment. \""
					+ AZURE_STORAGE_KEY_ENV_VARIABLE + "\" not set.");
		}
	}
}
