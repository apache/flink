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

import java.net.URL;
import java.util.Collection;

/**
 * {@link LibraryCacheManager} implementation which returns the context class loader.
 */
public enum ContextClassLoaderLibraryCacheManager implements LibraryCacheManager {
	INSTANCE;

	@Override
	public ClassLoader getClassLoader(JobID id) {
		return getClass().getClassLoader();
	}

	@Override
	public void registerJob(JobID id, Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths) {

	}

	@Override
	public void registerTask(JobID id, ExecutionAttemptID execution, Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths) {

	}

	@Override
	public void unregisterTask(JobID id, ExecutionAttemptID execution) {

	}

	@Override
	public void unregisterJob(JobID id) {

	}

	@Override
	public void shutdown() {

	}

	@Override
	public boolean hasClassLoader(@Nonnull JobID jobId) {
		return true;
	}
}
