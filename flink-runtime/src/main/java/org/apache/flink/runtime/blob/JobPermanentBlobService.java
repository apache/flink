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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;

/**
 * {@link PermanentBlobService} extension that gives access to register and release job artifacts.
 */
public interface JobPermanentBlobService extends PermanentBlobService {
    /**
     * Register the given job.
     *
     * @param jobId job id identifying the job to register
     * @param applicationId application id identifying the application the job belongs to
     */
    void registerJob(JobID jobId, ApplicationID applicationId);

    /**
     * Release the given job. This makes the blobs stored for this job up for cleanup.
     *
     * @param jobId job id identifying the job to register
     * @param applicationId application id identifying the application the job belongs to
     */
    void releaseJob(JobID jobId, ApplicationID applicationId);
}
