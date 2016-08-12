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

package org.apache.flink.runtime.clusterframework.types;

import java.io.Serializable;

public class ResourceProfile implements Serializable {

	private static final long serialVersionUID = -784900073893060124L;

	/** How many cpu cores are needed, use double so we can specify cpu like 0.1 */
	private final double cpuCores;

	/** How many memory in mb are needed */
	private final long memoryInMB;

	public ResourceProfile(double cpuCores, long memoryInMB) {
		this.cpuCores = cpuCores;
		this.memoryInMB = memoryInMB;
	}

	/**
	 * Check whether required resource profile can be matched
	 *
	 * @param required the required resource profile
	 * @return true if the requirement is matched, otherwise false
	 */
	public boolean matchRequirement(ResourceProfile required) {
		return Double.compare(cpuCores, required.cpuCores) >= 0 && memoryInMB >= required.memoryInMB;
	}
}
