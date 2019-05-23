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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;

public interface LibraryCacheManager {

	/**
	 * Returns the user code class loader associated with id.
	 *
	 * @param id identifying the job
	 * @return ClassLoader which can load the user code
	 */
	ClassLoader getClassLoader(JobID id);

	/**
	 * Registers a job with its required jar files and classpaths. The jar files are identified by
	 * their blob keys and downloaded for use by a {@link ClassLoader}.
	 *
	 * @param id job ID
	 * @param requiredJarFiles collection of blob keys identifying the required jar files
	 * @param requiredClasspaths collection of classpaths that are added to the user code class loader
	 *
	 * @throws IOException if any error occurs when retrieving the required jar files
	 *
	 * @see #unregisterJob(JobID) counterpart of this method
	 */
	void registerJob(JobID id, Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths)
		throws IOException;

	/**
	 * Registers a job task execution with its required jar files and classpaths. The jar files are
	 * identified by their blob keys and downloaded for use by a {@link ClassLoader}.
	 *
	 * @param id job ID
	 * @param requiredJarFiles collection of blob keys identifying the required jar files
	 * @param requiredClasspaths collection of classpaths that are added to the user code class loader
	 *
	 * @throws IOException if any error occurs when retrieving the required jar files
	 *
	 * @see #unregisterTask(JobID, ExecutionAttemptID) counterpart of this method
	 */
	void registerTask(JobID id, ExecutionAttemptID execution, Collection<PermanentBlobKey> requiredJarFiles,
		Collection<URL> requiredClasspaths) throws IOException;

	/**
	 * Unregisters a job task execution from the library cache manager.
	 * <p>
	 * <strong>Note:</strong> this is the counterpart of {@link #registerTask(JobID,
	 * ExecutionAttemptID, Collection, Collection)} and it will not remove any job added via
	 * {@link #registerJob(JobID, Collection, Collection)}!
	 *
	 * @param id job ID
	 *
	 * @see #registerTask(JobID, ExecutionAttemptID, Collection, Collection) counterpart of this method
	 */
	void unregisterTask(JobID id, ExecutionAttemptID execution);

	/**
	 * Unregisters a job from the library cache manager.
	 * <p>
	 * <strong>Note:</strong> this is the counterpart of {@link #registerJob(JobID, Collection,
	 * Collection)} and it will not remove any job task execution added via {@link
	 * #registerTask(JobID, ExecutionAttemptID, Collection, Collection)}!
	 *
	 * @param id job ID
	 *
	 * @see #registerJob(JobID, Collection, Collection) counterpart of this method
	 */
	void unregisterJob(JobID id);

	/**
	 * Shutdown method which may release created class loaders.
	 */
	void shutdown();

	/**
	 * True if the LibraryCacheManager has a user code class loader registered
	 * for the given job id.
	 *
	 * @param jobId identifying the job for which to check the class loader
	 * @return true if the user code class loader for the given job has been registered. Otherwise false.
	 */
	boolean hasClassLoader(@Nonnull JobID jobId);
}
