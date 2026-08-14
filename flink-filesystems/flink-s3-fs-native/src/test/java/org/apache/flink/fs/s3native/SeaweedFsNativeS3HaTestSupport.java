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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.testutils.CommonTestUtils;

import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared helpers for the HA IT cases ({@link HAJobRunOnNativeS3FileSystemITCase}, {@link
 * HAApplicationRunOnNativeS3FileSystemITCase}) that store their HA data in a {@link
 * SeaweedFsNativeS3TestContainer}.
 */
final class SeaweedFsNativeS3HaTestSupport {

    static final String CLUSTER_ID = "test-cluster";

    private SeaweedFsNativeS3HaTestSupport() {}

    static String s3UriWithSubPath(SeaweedFsNativeS3TestContainer container, String... subfolders) {
        return container.getS3UriForDefaultBucket() + "/" + String.join("/", subfolders);
    }

    static Configuration baseResultStoreConfiguration(
            SeaweedFsNativeS3TestContainer container,
            ConfigOption<Boolean> deleteOnCommitOption,
            ConfigOption<String> storagePathOption,
            String resultStoreFolder) {
        final Configuration config = new Configuration();
        container.setS3ConfigOptions(config);
        config.set(deleteOnCommitOption, Boolean.FALSE);
        config.set(storagePathOption, s3UriWithSubPath(container, CLUSTER_ID, resultStoreFolder));
        return config;
    }

    static List<S3Object> listResultStoreObjects(
            SeaweedFsNativeS3TestContainer container, String resultStoreFolder) {
        return container.listObjects(String.join("/", CLUSTER_ID, resultStoreFolder));
    }

    static void assertSingleCleanResultStoreEntry(
            SeaweedFsNativeS3TestContainer container,
            String resultStoreFolder,
            Predicate<String> isValidEntry,
            Predicate<String> isDirtyEntry,
            String expectedStatus)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final List<S3Object> objects =
                            listResultStoreObjects(container, resultStoreFolder);
                    return objects.stream().map(S3Object::key).anyMatch(isValidEntry)
                            && objects.stream().map(S3Object::key).noneMatch(isDirtyEntry);
                },
                2000L);

        final List<S3Object> objects = listResultStoreObjects(container, resultStoreFolder);
        assertThat(objects).hasSize(1);

        final String key = objects.get(0).key();
        assertThat(key).matches(isValidEntry).doesNotMatch(isDirtyEntry);

        assertThat(container.getObjectAsString(key)).contains(expectedStatus);
    }

    static void unsetFileSystem() {
        FileSystem.initialize(new Configuration(), null);
    }
}
