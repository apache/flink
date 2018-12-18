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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

import java.util.concurrent.CompletableFuture;

/**
 * Oracle for {@link KvStateLocation} in the cluster. In order to answer {@link InternalKvState}
 * location requests, the {@link TaskExecutor} have to register and unregister their respective
 * key-value states at the oracle.
 */
public interface KvStateLocationOracle {

	/**
	 * Requests a {@link KvStateLocation} for the specified {@link InternalKvState} registration name.
	 *
	 * @param jobId identifying the job for which to request the {@link KvStateLocation}
	 * @param registrationName Name under which the KvState has been registered.
	 * @return Future of the requested {@link InternalKvState} location
	 */
	CompletableFuture<KvStateLocation> requestKvStateLocation(
		final JobID jobId,
		final String registrationName);
}
