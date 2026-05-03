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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the {@link ArchivePathUtils}. */
public class ArchivePathUtilsTest {

    private Configuration configuration;

    @BeforeEach
    void setUp() {
        configuration = new Configuration();
        configuration.set(JobManagerOptions.ARCHIVE_DIR, "/archive");
        configuration.set(ClusterOptions.CLUSTER_ID, "cluster123");
    }

    @Test
    void testGetJobArchivePath_withoutApplicationId() {
        JobID jobId = new JobID();

        Path result = ArchivePathUtils.getJobArchivePath(configuration, jobId, null);

        Path expected = new Path("/archive", jobId.toHexString());
        assertEquals(expected, result);
    }

    @Test
    void testGetJobArchivePath_withApplicationId() {
        JobID jobId = new JobID();
        ApplicationID applicationId = new ApplicationID();

        Path result = ArchivePathUtils.getJobArchivePath(configuration, jobId, applicationId);

        Path expected =
                new Path(
                        "/archive/cluster123/applications/"
                                + applicationId.toHexString()
                                + "/jobs/"
                                + jobId.toHexString());

        assertEquals(expected, result);
    }

    @Test
    void testGetApplicationArchivePath() {
        ApplicationID applicationId = new ApplicationID();

        Path result = ArchivePathUtils.getApplicationArchivePath(configuration, applicationId);

        Path expected =
                new Path(
                        "/archive/cluster123/applications/"
                                + applicationId.toHexString()
                                + "/application-summary");

        assertEquals(expected, result);
    }
}
