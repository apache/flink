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
import org.apache.flink.runtime.util.HadoopConfigLoader;
import org.apache.flink.util.Preconditions;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystemFactory} interface for
 * Google Storage.
 */
public class GSFileSystemFactory implements FileSystemFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(GSFileSystemFactory.class);

    /** The scheme for the Google Storage file system. */
    public static final String SCHEME = "gs";

    private static final String HADOOP_CONFIG_PREFIX = "fs.gs.";

    private static final String[] FLINK_CONFIG_PREFIXES = {"gs.", HADOOP_CONFIG_PREFIX};

    private static final String[][] MIRRORED_CONFIG_KEYS = {};

    private static final String FLINK_SHADING_PREFIX = "";

    private final HadoopConfigLoader hadoopConfigLoader;

    @Nullable private Configuration flinkConfig;

    /** Constructs the Google Storage file system factory. */
    public GSFileSystemFactory() {
        LOGGER.info("Creating GSFileSystemFactory");

        this.hadoopConfigLoader =
                new HadoopConfigLoader(
                        FLINK_CONFIG_PREFIXES,
                        MIRRORED_CONFIG_KEYS,
                        HADOOP_CONFIG_PREFIX,
                        Collections.emptySet(),
                        Collections.emptySet(),
                        FLINK_SHADING_PREFIX);
    }

    @Override
    public void configure(Configuration flinkConfig) {
        LOGGER.info("Configuring GSFileSystemFactory with Flink configuration {}", flinkConfig);

        this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
        hadoopConfigLoader.setFlinkConfig(flinkConfig);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        LOGGER.info("Creating GS file system for uri {}", fsUri);

        Preconditions.checkNotNull(fsUri);

        // create and configure the Google Hadoop file system
        org.apache.hadoop.conf.Configuration hadoopConfig =
                hadoopConfigLoader.getOrLoadHadoopConfig();
        LOGGER.info(
                "Creating GoogleHadoopFileSystem for uri {} with Hadoop config {}",
                fsUri,
                hadoopConfig);
        GoogleHadoopFileSystem googleHadoopFileSystem = new GoogleHadoopFileSystem();
        googleHadoopFileSystem.initialize(fsUri, hadoopConfig);

        // construct the file system options
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);

        // create the file system wrapper
        return new GSFileSystem(googleHadoopFileSystem, options);
    }
}
