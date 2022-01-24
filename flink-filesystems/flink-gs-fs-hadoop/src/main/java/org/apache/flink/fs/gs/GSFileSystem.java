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

package org.apache.flink.fs.gs;

import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gs.storage.GSBlobStorageImpl;
import org.apache.flink.fs.gs.writer.GSRecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.Preconditions;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

/** Provides recoverable-writer functionality for the standard GoogleHadoopFileSystem. */
class GSFileSystem extends HadoopFileSystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(GSFileSystem.class);

    private final GSFileSystemOptions options;

    GSFileSystem(GoogleHadoopFileSystem googleHadoopFileSystem, GSFileSystemOptions options) {
        super(Preconditions.checkNotNull(googleHadoopFileSystem));
        this.options = Preconditions.checkNotNull(options);
        LOGGER.info("Creating GSFileSystem with options {}", options);
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {

        // follow the same rules as for the Hadoop connector, i.e.
        // 1) only use service credentials at all if Hadoop
        // "google.cloud.auth.service.account.enable" is true (default: true)
        // 2) use GOOGLE_APPLICATION_CREDENTIALS as location of credentials, if supplied
        // 3) use Hadoop "google.cloud.auth.service.account.json.keyfile" as location of
        // credentials, if supplied
        // 4) use no credentials

        // store any credentials we are to use, here
        Optional<String> credentialsPath = Optional.empty();

        // only look for credentials if service account support is enabled
        Configuration hadoopConfig = getHadoopFileSystem().getConf();
        boolean enableServiceAccount =
                hadoopConfig.getBoolean("google.cloud.auth.service.account.enable", true);
        if (enableServiceAccount) {

            // load google application credentials, and then fall back to
            // "google.cloud.auth.service.account.json.keyfile" from Hadoop
            credentialsPath = Optional.ofNullable(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
            if (credentialsPath.isPresent()) {
                LOGGER.info(
                        "Recoverable writer is using GOOGLE_APPLICATION_CREDENTIALS at {}",
                        credentialsPath.get());
            } else {
                credentialsPath =
                        Optional.ofNullable(
                                hadoopConfig.get("google.cloud.auth.service.account.json.keyfile"));
                credentialsPath.ifPresent(
                        path ->
                                LOGGER.info(
                                        "Recoverable writer is using credentials from Hadoop at {}",
                                        path));
            }
        }

        // construct the storage instance, using credentials if provided
        Storage storage;
        if (credentialsPath.isPresent()) {
            LOGGER.info(
                    "Creating GSRecoverableWriter using credentials from {}",
                    credentialsPath.get());
            try (FileInputStream credentialsStream = new FileInputStream(credentialsPath.get())) {
                GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsStream);
                storage =
                        StorageOptions.newBuilder()
                                .setCredentials(credentials)
                                .build()
                                .getService();
            }
        } else {
            LOGGER.info("Creating GSRecoverableWriter using no credentials");
            storage = StorageOptions.newBuilder().build().getService();
        }

        // create the GS blob storage wrapper
        GSBlobStorageImpl blobStorage = new GSBlobStorageImpl(storage);

        // construct the recoverable writer with the blob storage wrapper and the options
        return new GSRecoverableWriter(blobStorage, options);
    }
}
