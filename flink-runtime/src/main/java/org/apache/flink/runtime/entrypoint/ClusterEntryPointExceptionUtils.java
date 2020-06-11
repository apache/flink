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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.JobManagerOptions;

import javax.annotation.Nullable;

import static org.apache.flink.util.ExceptionUtils.tryEnrichOutOfMemoryError;

/**
 * Exception utils to handle and enrich exceptions occurring in TaskManager.
 */
class ClusterEntryPointExceptionUtils {
	private static final String JM_DIRECT_OOM_ERROR_MESSAGE = String.format(
		"Direct buffer memory. The direct out-of-memory error has occurred. This can mean two things: either Flink Master requires " +
			"a larger size of JVM direct memory or there is a direct memory leak. The direct memory can be " +
			"allocated by user code or some of its dependencies. Flink framework and its dependencies also consume " +
			"the direct memory, mostly for network communication. In certain special cases, in particular for jobs with " +
			"high parallelism, the framework may require more direct memory which is not managed by Flink. " +
			"To increase the JVM Direct Memory limit, '%s' configuration option should be increased. " +
			"If the error persists then there is probably a direct memory leak in user code or some of its dependencies " +
			"which has to be investigated and fixed. The Flink Master has to be shutdown...",
		JobManagerOptions.OFF_HEAP_MEMORY.key());

	private static final String JM_METASPACE_OOM_ERROR_MESSAGE = String.format(
		"Metaspace. The metaspace out-of-memory error has occurred. This can mean two things: either Flink Master requires " +
			"a larger size of JVM metaspace to load classes or there is a class loading leak. In the first case " +
			"'%s' configuration option should be increased. If the error persists (usually in cluster after " +
			"several job (re-)submissions) then there is probably a class loading leak in user code or some of its dependencies " +
			"which has to be investigated and fixed. The Flink Master has to be shutdown...",
		JobManagerOptions.JVM_METASPACE.key());

	private ClusterEntryPointExceptionUtils() {
	}

	/**
	 * Tries to enrich the passed exception with additional information.
	 *
	 * <p>This method improves error message for direct and metaspace {@link OutOfMemoryError}.
	 * It adds description of possible causes and ways of resolution.
	 *
	 * @param exception exception to enrich if not {@code null}
	 * @return the enriched exception or the original if no additional information could be added;
	 * {@code null} if the argument was {@code null}
	 */
	@Nullable
	static Throwable tryEnrichClusterEntryPointError(@Nullable Throwable exception) {
		return tryEnrichOutOfMemoryError(exception, JM_METASPACE_OOM_ERROR_MESSAGE, JM_DIRECT_OOM_ERROR_MESSAGE);
	}
}

