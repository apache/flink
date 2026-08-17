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
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.highavailability.AbstractHAJobRunITCase;
import org.apache.flink.runtime.highavailability.FileSystemJobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs {@link AbstractHAJobRunITCase} with HA data stored in SeaweedFS via the native S3 FS. */
class HAJobRunOnNativeS3FileSystemITCase extends AbstractHAJobRunITCase {

    private static final String CLUSTER_ID = "test-cluster";
    private static final String JOB_RESULT_STORE_FOLDER = "jrs";

    @RegisterExtension
    @Order(2)
    private static final AllCallbackWrapper<TestContainerExtension<SeaweedFsNativeS3TestContainer>>
            SEAWEEDFS_EXTENSION =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(SeaweedFsNativeS3TestContainer::new));

    @RegisterExtension
    @Order(3)
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    () -> {
                        final Configuration configuration = createConfiguration();
                        FileSystem.initialize(configuration, null);
                        return new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .build();
                    });

    private static SeaweedFsNativeS3TestContainer getSeaweedFsContainer() {
        return SEAWEEDFS_EXTENSION.getCustomExtension().getTestContainer();
    }

    private static Configuration createConfiguration() {
        final Configuration config = new Configuration();

        getSeaweedFsContainer().setS3ConfigOptions(config);

        config.set(JobResultStoreOptions.DELETE_ON_COMMIT, Boolean.FALSE);
        config.set(
                JobResultStoreOptions.STORAGE_PATH,
                createS3URIWithSubPath(CLUSTER_ID, JOB_RESULT_STORE_FOLDER));

        return addHaConfiguration(config, createS3URIWithSubPath(CLUSTER_ID));
    }

    private static String createS3URIWithSubPath(String... subfolders) {
        return getSeaweedFsContainer().getS3UriForDefaultBucket()
                + "/"
                + String.join("/", subfolders);
    }

    private static List<S3Object> getObjectsFromJobResultStore() {
        return getSeaweedFsContainer()
                .listObjects(String.join("/", CLUSTER_ID, JOB_RESULT_STORE_FOLDER));
    }

    @AfterAll
    static void unsetFileSystem() {
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected void runAfterJobTermination() throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final List<S3Object> objects = getObjectsFromJobResultStore();
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

        final List<S3Object> objects = getObjectsFromJobResultStore();
        assertThat(objects).hasSize(1);

        final String key = objects.get(0).key();
        assertThat(key)
                .matches(FileSystemJobResultStore::hasValidJobResultStoreEntryExtension)
                .doesNotMatch(FileSystemJobResultStore::hasValidDirtyJobResultStoreEntryExtension);

        assertThat(getSeaweedFsContainer().getObjectAsString(key))
                .contains(ApplicationStatus.SUCCEEDED.name());
    }
}
