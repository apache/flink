/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Container for multiple {@link JobManagerConnection} registered under their respective job id.
 */
public interface JobManagerTable {

	/**
	 * Checks whether a {@link JobManagerConnection} for the given {@link JobID} is contained.
	 *
	 * @param jobId jobId which identifies the {@link JobManagerConnection}
	 * @return true if a {@link JobManagerConnection} for the given {@link JobID} is contained
	 */
	boolean contains(JobID jobId);

	/**
	 * Checks whether a {@link JobManagerConnection} for the given {@link ResourceID} is contained.
	 *
	 * @param resourceId resourceId identifying the job manager connection
	 * @return true if a {@link JobManagerConnection} for the given {@link ResourceID} is contained
	 */
	boolean contains(ResourceID resourceId);

	/**
	 * Puts a {@link JobManagerConnection} under the given {@link JobID} and {@link ResourceID} into the table.
	 *
	 * @param jobId jobId identifying the {@link JobManagerConnection}
	 * @param resourceId resourceId identifying the {@link JobManagerConnection}
	 * @param jobManagerConnection jobManagerConnection which is stored under the given {@link JobID}
	 * @return true if the {@link JobManagerConnection} could be added to the table; otherwise false
	 */
	boolean put(JobID jobId, ResourceID resourceId, JobManagerConnection jobManagerConnection);

	/**
	 * Removes the {@link JobManagerConnection} stored under the given {@link JobID}.
	 *
	 * @param jobId jobId identifying the {@link JobManagerConnection} to remove
	 * @return the removed {@link JobManagerConnection}; {@code null} if none has been contained
	 */
	@Nullable
	JobManagerConnection remove(JobID jobId);

	/**
	 * Gets the {@link JobManagerConnection} stored under the given {@link JobID}.
	 *
	 * @param jobId jobId identifying the {@link JobManagerConnection} to return
	 * @return the {@link JobManagerConnection} stored under jobId; {@code null} if non has been stored
	 */
	@Nullable
	JobManagerConnection get(JobID jobId);

	/**
	 * Gets the {@link JobManagerConnection} stored under the given {@link ResourceID}.
	 *
	 * @param resourceId resourceId identifying the {@link JobManagerConnection} to return
	 * @return the {@link JobManagerConnection} stored under resourceId; {@code null} otherwise
	 */
	@Nullable
	JobManagerConnection get(ResourceID resourceId);

	/**
	 * Returns a collection of all contained {@link JobManagerConnection}.
	 *
	 * @return collection of all contained {@link JobManagerConnection}
	 */
	Collection<JobManagerConnection> values();
}
