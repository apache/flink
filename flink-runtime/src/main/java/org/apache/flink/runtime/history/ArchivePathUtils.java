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

package org.apache.flink.runtime.history;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

/** Utils for archive path. */
public class ArchivePathUtils {

    public static final String JOBS_DIR = "jobs";

    public static final String APPLICATIONS_DIR = "applications";

    public static final String APPLICATION_ARCHIVE_NAME = "application-summary";

    public static Path getJobArchivePath(
            Configuration configuration, JobID jobId, @Nullable ApplicationID applicationId) {
        String archiveDir = configuration.get(JobManagerOptions.ARCHIVE_DIR);
        String clusterId = configuration.get(ClusterOptions.CLUSTER_ID);
        if (applicationId == null) {
            return new Path(archiveDir, jobId.toHexString());
        } else {
            Path applicationDir = getApplicationDir(archiveDir, clusterId, applicationId);
            return new Path(new Path(applicationDir, JOBS_DIR), jobId.toHexString());
        }
    }

    public static Path getApplicationArchivePath(
            Configuration configuration, ApplicationID applicationId) {
        String archiveDir = configuration.get(JobManagerOptions.ARCHIVE_DIR);
        String clusterId = configuration.get(ClusterOptions.CLUSTER_ID);
        Path applicationDir = getApplicationDir(archiveDir, clusterId, applicationId);
        return new Path(applicationDir, APPLICATION_ARCHIVE_NAME);
    }

    private static Path getApplicationDir(
            String archiveDir, String clusterId, ApplicationID applicationId) {
        Path clusterDir = new Path(archiveDir, clusterId);
        return new Path(new Path(clusterDir, APPLICATIONS_DIR), applicationId.toHexString());
    }
}
