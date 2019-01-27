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

package org.apache.flink.runtime.jobmaster.failover;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;

/**
 * The interface for components that can replay operation log.
 */
public class OperationLogStoreLoader {

	/**
	 * Load operation log store according to the configuration.
	 */
	public static OperationLogStore loadOperationLogStore(JobID jobID, Configuration config) {

		final String storeParam = config.getString(JobManagerOptions.OPERATION_LOG_STORE);

		switch (storeParam.toLowerCase()) {
			case "dummy":
				return new DummyOperationLogStore();
			case "memory":
				return new MemoryOperationLogStore();
			case "filesystem":
				return new FileSystemOperationLogStore(jobID, config);
			default:
				return new DummyOperationLogStore();
		}
	}
}
