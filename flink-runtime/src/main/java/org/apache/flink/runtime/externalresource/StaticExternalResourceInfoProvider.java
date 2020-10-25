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

package org.apache.flink.runtime.externalresource;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Static implementation of {@link ExternalResourceInfoProvider} which return fixed collection
 * of {@link ExternalResourceInfo}.
 */
public class StaticExternalResourceInfoProvider implements ExternalResourceInfoProvider {

	private final Map<String, Set<? extends ExternalResourceInfo>> externalResources;

	public StaticExternalResourceInfoProvider(Map<String, Set<? extends ExternalResourceInfo>> externalResources) {
		this.externalResources = externalResources;
	}

	@Override
	public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
		if (!externalResources.containsKey(resourceName)) {
			return Collections.emptySet();
		}

		return Collections.unmodifiableSet(externalResources.get(resourceName));
	}

	@VisibleForTesting
	Map<String, Set<? extends ExternalResourceInfo>> getExternalResources() {
		return externalResources;
	}
}
