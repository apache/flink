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

package org.apache.flink.client.deployment.application.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;

/**
 * An interface to be implemented by {@link JobClient} suppliers.
 */
@Internal
public interface EmbeddedJobClientCreator {

	/**
	 * Creates a {@link JobClient} that is adequate for the context in which the job is executed.
	 * @param jobId the job id of the job associated with the returned client.
	 * @param userCodeClassloader the class loader to deserialize user code.
	 * @return the job client.
	 */
	JobClient getJobClient(final JobID jobId, final ClassLoader userCodeClassloader);
}
