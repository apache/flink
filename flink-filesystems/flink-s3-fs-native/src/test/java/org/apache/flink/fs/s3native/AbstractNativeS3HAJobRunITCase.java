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

package org.apache.flink.fs.s3native;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.highavailability.AbstractHAJobRunITCase;
import org.apache.flink.runtime.highavailability.FileSystemJobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.junit.jupiter.api.AfterAll;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs {@link AbstractHAJobRunITCase} with the HA data stored in a native-S3-backed {@link
 * SeaweedFsNativeS3TestContainer}.
 */
abstract class AbstractNativeS3HAJobRunITCase extends AbstractHAJobRunITCase {

    static final String CLUSTER_ID = "test-cluster";
    private static final String JOB_RESULT_STORE_FOLDER = "jrs";

    /** Provided by the concrete runner that owns the SeaweedFS cluster extension. */
    abstract SeaweedFsNativeS3TestContainer getSeaweedFsContainer();

    static Configuration createConfiguration(SeaweedFsNativeS3TestContainer container) {
        final Configuration config = new Configuration();
        container.setS3ConfigOptions(config);
        config.set(JobResultStoreOptions.DELETE_ON_COMMIT, Boolean.FALSE);
        config.set(
                JobResultStoreOptions.STORAGE_PATH,
                s3UriWithSubPath(container, CLUSTER_ID, JOB_RESULT_STORE_FOLDER));
        return addHaConfiguration(config, s3UriWithSubPath(container, CLUSTER_ID));
    }

    private static String s3UriWithSubPath(
            SeaweedFsNativeS3TestContainer container, String... subfolders) {
        return container.getS3UriForDefaultBucket() + "/" + String.join("/", subfolders);
    }

    @AfterAll
    static void unsetFileSystem() {
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected void runAfterJobTermination() throws Exception {
        final SeaweedFsNativeS3TestContainer container = getSeaweedFsContainer();
        final String prefix = String.join("/", CLUSTER_ID, JOB_RESULT_STORE_FOLDER);

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final List<S3Object> objects = container.listObjects(prefix);
                    return objects.stream()
                                    .map(S3Object::key)
                                    .anyMatch(
                                            FileSystemJobResultStore
                                                    ::hasValidJobResultStoreEntryExtension)
                            && objects.stream()
                                    .map(S3Object::key)
                                    .noneMatch(
                                            FileSystemJobResultStore
                                                    ::hasValidDirtyJobResultStoreEntryExtension);
                },
                2000L);

        final List<S3Object> objects = container.listObjects(prefix);
        assertThat(objects).hasSize(1);

        final String key = objects.get(0).key();
        assertThat(key)
                .matches(FileSystemJobResultStore::hasValidJobResultStoreEntryExtension)
                .doesNotMatch(FileSystemJobResultStore::hasValidDirtyJobResultStoreEntryExtension);

        assertThat(container.getObjectAsString(key)).contains(ApplicationStatus.SUCCEEDED.name());
    }
}
