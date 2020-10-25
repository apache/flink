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

import org.apache.flink.api.common.externalresource.ExternalResourceInfo;

import java.util.Collections;
import java.util.Set;

/**
 * Provide the information of external resources.
 */
public interface ExternalResourceInfoProvider {

	ExternalResourceInfoProvider NO_EXTERNAL_RESOURCES = resourceName -> Collections.emptySet();

	/**
	 * Get the specific external resource information by the resourceName.
	 *
	 * @param resourceName of the required external resource
	 * @return information set of the external resource identified by the resourceName
	 */
	Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName);
}
