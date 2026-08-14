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

import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.AbstractHAApplicationRunITCase;
import org.apache.flink.runtime.highavailability.ApplicationResultStoreOptions;
import org.apache.flink.runtime.highavailability.FileSystemApplicationResultStore;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Runs {@link AbstractHAApplicationRunITCase} with HA data stored in SeaweedFS via the native S3
 * FS.
 */
class HAApplicationRunOnNativeS3FileSystemITCase extends AbstractHAApplicationRunITCase {

    private static final String CLUSTER_ID = SeaweedFsNativeS3HaTestSupport.CLUSTER_ID;
    private static final String APPLICATION_RESULT_STORE_FOLDER = "ars";

    // AbstractHAApplicationRunITCase already registers its own extension at @Order(1), so this
    // one must run after it.
    @RegisterExtension
    @Order(2)
    private static final SeaweedFsNativeS3HAClusterExtension CLUSTER_EXTENSION =
            new SeaweedFsNativeS3HAClusterExtension(
                    HAApplicationRunOnNativeS3FileSystemITCase::createConfiguration);

    private static SeaweedFsNativeS3TestContainer getSeaweedFsContainer() {
        return CLUSTER_EXTENSION.getContainer();
    }

    private static Configuration createConfiguration(SeaweedFsNativeS3TestContainer container) {
        final Configuration config =
                SeaweedFsNativeS3HaTestSupport.baseResultStoreConfiguration(
                        container,
                        ApplicationResultStoreOptions.DELETE_ON_COMMIT,
                        ApplicationResultStoreOptions.STORAGE_PATH,
                        APPLICATION_RESULT_STORE_FOLDER);
        return addHaConfiguration(
                config, SeaweedFsNativeS3HaTestSupport.s3UriWithSubPath(container, CLUSTER_ID));
    }

    @AfterAll
    static void unsetFileSystem() {
        SeaweedFsNativeS3HaTestSupport.unsetFileSystem();
    }

    @Override
    protected void runAfterApplicationTermination() throws Exception {
        SeaweedFsNativeS3HaTestSupport.assertSingleCleanResultStoreEntry(
                getSeaweedFsContainer(),
                APPLICATION_RESULT_STORE_FOLDER,
                FileSystemApplicationResultStore::hasValidApplicationResultStoreEntryExtension,
                FileSystemApplicationResultStore::hasValidDirtyApplicationResultStoreEntryExtension,
                ApplicationState.FINISHED.name());
    }
}
