/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.services.memorymanager;

import java.util.ServiceLoader;

public abstract class MemoryManagementProvider {
	private static MemoryManagementProvider defaultProvider = null;

	public static MemoryManagementProvider getDefault() {
		if (defaultProvider != null) {
			return defaultProvider;
		}

		ServiceLoader<MemoryManagementProvider> ldr = ServiceLoader.load(MemoryManagementProvider.class);

		for (MemoryManagementProvider provider : ldr) {
			defaultProvider = provider;
			return provider;
		}

		throw new Error("No HelloProvider registered");
	}
}
