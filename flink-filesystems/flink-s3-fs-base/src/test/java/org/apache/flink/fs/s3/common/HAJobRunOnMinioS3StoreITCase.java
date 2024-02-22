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

package org.apache.flink.fs.s3.common;

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

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.apache.flink.shaded.guava31.com.google.common.base.Predicates.not;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code HAJobRunOnMinioS3StoreITCase} covers a job run where the HA data is stored in Minio. The
 * implementation verifies whether the {@code JobResult} was written into the FileSystem-backed
 * {@code JobResultStore}.
 */
public abstract class HAJobRunOnMinioS3StoreITCase extends AbstractHAJobRunITCase {

    private static final String CLUSTER_ID = "test-cluster";
    private static final String JOB_RESULT_STORE_FOLDER = "jrs";

    @RegisterExtension
    @Order(2)
    private static final AllCallbackWrapper<TestContainerExtension<MinioTestContainer>>
            MINIO_EXTENSION =
                    new AllCallbackWrapper<>(new TestContainerExtension<>(MinioTestContainer::new));

    @RegisterExtension
    @Order(3)
    private static final MiniClusterExtension miniClusterExtension =
            new MiniClusterExtension(
                    () -> {
                        final Configuration configuration = createConfiguration();
                        FileSystem.initialize(configuration, null);
                        return new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .build();
                    });

    private static MinioTestContainer getMinioContainer() {
        return MINIO_EXTENSION.getCustomExtension().getTestContainer();
    }

    private static String createS3URIWithSubPath(String... subfolders) {
        return getMinioContainer().getS3UriForDefaultBucket() + createSubPath(subfolders);
    }

    private static List<S3ObjectSummary> getObjectsFromJobResultStore() {
        return getMinioContainer()
                .getClient()
                .listObjects(
                        getMinioContainer().getDefaultBucketName(),
                        createSubPath(CLUSTER_ID, JOB_RESULT_STORE_FOLDER))
                .getObjectSummaries();
    }

    private static String createSubPath(String... subfolders) {
        final String pathSeparator = "/";
        return pathSeparator + StringUtils.join(subfolders, pathSeparator);
    }

    private static Configuration createConfiguration() {
        final Configuration config = new Configuration();

        getMinioContainer().setS3ConfigOptions(config);

        // JobResultStore configuration
        config.set(JobResultStoreOptions.DELETE_ON_COMMIT, Boolean.FALSE);
        config.set(
                JobResultStoreOptions.STORAGE_PATH,
                createS3URIWithSubPath(CLUSTER_ID, JOB_RESULT_STORE_FOLDER));

        return addHaConfiguration(config, createS3URIWithSubPath(CLUSTER_ID));
    }

    @AfterAll
    public static void unsetFileSystem() {
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected void runAfterJobTermination() throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final List<S3ObjectSummary> objects = getObjectsFromJobResultStore();
                    return objects.stream()
                                    .map(S3ObjectSummary::getKey)
                                    .anyMatch(
                                            FileSystemJobResultStore
                                                    ::hasValidJobResultStoreEntryExtension)
                            && objects.stream()
                                    .map(S3ObjectSummary::getKey)
                                    .noneMatch(
                                            FileSystemJobResultStore
                                                    ::hasValidDirtyJobResultStoreEntryExtension);
                },
                2000L);

        final S3ObjectSummary objRef = Iterables.getOnlyElement(getObjectsFromJobResultStore());
        assertThat(objRef.getKey())
                .matches(FileSystemJobResultStore::hasValidJobResultStoreEntryExtension)
                .matches(not(FileSystemJobResultStore::hasValidDirtyJobResultStoreEntryExtension));

        final String objContent =
                getMinioContainer()
                        .getClient()
                        .getObjectAsString(objRef.getBucketName(), objRef.getKey());
        assertThat(objContent).contains(ApplicationStatus.SUCCEEDED.name());
    }
}
