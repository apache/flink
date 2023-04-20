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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.fs.gs.utils.ConfigUtils;
import org.apache.flink.util.Preconditions;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystemFactory} interface for
 * Google Storage.
 */
public class GSFileSystemFactory implements FileSystemFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(GSFileSystemFactory.class);

    /** The scheme for the Google Storage file system. */
    public static final String SCHEME = "gs";

    /**
     * The Hadoop, formed by combining system Hadoop config with properties defined in Flink config.
     */
    @Nullable private org.apache.hadoop.conf.Configuration hadoopConfig;

    /** The options used for GSFileSystem and RecoverableWriter. */
    @Nullable private GSFileSystemOptions fileSystemOptions;

    /**
     * Though it isn't documented as clearly as one might expect, the methods on this object are
     * threadsafe, so we can safely share a single instance among all file system instances.
     *
     * <p>Issue that discusses pending docs is here:
     * https://github.com/googleapis/google-cloud-java/issues/1238
     *
     * <p>StackOverflow discussion:
     * https://stackoverflow.com/questions/54516284/google-cloud-storage-java-client-pooling
     */
    @Nullable private Storage storage;

    /** Constructs the Google Storage file system factory. */
    public GSFileSystemFactory() {
        LOGGER.info("Creating GSFileSystemFactory");
    }

    @Override
    public void configure(Configuration flinkConfig) {
        LOGGER.info("Configuring GSFileSystemFactory with Flink configuration {}", flinkConfig);

        Preconditions.checkNotNull(flinkConfig);

        ConfigUtils.ConfigContext configContext = new RuntimeConfigContext();

        // load Hadoop config
        this.hadoopConfig = ConfigUtils.getHadoopConfiguration(flinkConfig, configContext);
        LOGGER.info(
                "Using Hadoop configuration {}", ConfigUtils.stringifyHadoopConfig(hadoopConfig));

        // construct file-system options
        this.fileSystemOptions = new GSFileSystemOptions(flinkConfig);
        LOGGER.info("Using file system options {}", fileSystemOptions);

        // get storage credentials and construct Storage instance
        Optional<GoogleCredentials> credentials =
                ConfigUtils.getStorageCredentials(hadoopConfig, configContext);
        StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
        credentials.ifPresent(storageOptionsBuilder::setCredentials);
        this.storage = storageOptionsBuilder.build().getService();
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        LOGGER.info("Creating GSFileSystem for uri {} with options {}", fsUri, fileSystemOptions);

        Preconditions.checkNotNull(fsUri);

        // create the Google Hadoop file system
        GoogleHadoopFileSystem googleHadoopFileSystem = new GoogleHadoopFileSystem();
        try {
            googleHadoopFileSystem.initialize(fsUri, hadoopConfig);
        } catch (IOException ex) {
            throw new IOException("Failed to initialize GoogleHadoopFileSystem", ex);
        }

        // create the file system
        return new GSFileSystem(googleHadoopFileSystem, storage, fileSystemOptions);
    }

    /** Config context implementation used at runtime. */
    private static class RuntimeConfigContext implements ConfigUtils.ConfigContext {

        @Override
        public Optional<String> getenv(String name) {
            return Optional.ofNullable(System.getenv(name));
        }

        @Override
        public org.apache.hadoop.conf.Configuration loadHadoopConfigFromDir(String configDir) {
            org.apache.hadoop.conf.Configuration hadoopConfig =
                    new org.apache.hadoop.conf.Configuration();
            hadoopConfig.addResource(new Path(configDir, "core-default.xml"));
            hadoopConfig.addResource(new Path(configDir, "core-site.xml"));
            hadoopConfig.reloadConfiguration();
            return hadoopConfig;
        }

        @Override
        public GoogleCredentials loadStorageCredentialsFromFile(String credentialsPath) {
            try (FileInputStream credentialsStream = new FileInputStream(credentialsPath)) {
                return GoogleCredentials.fromStream(credentialsStream);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
