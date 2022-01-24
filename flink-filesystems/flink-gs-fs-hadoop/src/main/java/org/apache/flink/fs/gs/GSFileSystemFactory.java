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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.util.HadoopConfigLoader;
import org.apache.flink.util.Preconditions;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

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

    @Nullable private Configuration flinkConfig;

    @Nullable private org.apache.hadoop.conf.Configuration hadoopConfig;

    /** Constructs the Google Storage file system factory. */
    public GSFileSystemFactory() {
        LOGGER.info("Creating GSFileSystemFactory");
    }

    @Override
    public void configure(Configuration flinkConfig) {
        LOGGER.info("Configuring GSFileSystemFactory with Flink configuration {}", flinkConfig);

        this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
        this.hadoopConfig = getHadoopConfiguration(flinkConfig);

        LOGGER.info("Using Hadoop configuration {}", serializeHadoopConfig(hadoopConfig));
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        LOGGER.info("Creating GS file system for uri {}", fsUri);

        Preconditions.checkNotNull(fsUri);

        // initialize the Google Hadoop filesystem
        GoogleHadoopFileSystem googleHadoopFileSystem = new GoogleHadoopFileSystem();
        try {
            googleHadoopFileSystem.initialize(fsUri, hadoopConfig);
        } catch (IOException ex) {
            throw new IOException("Failed to initialize GoogleHadoopFileSystem", ex);
        }

        // construct the file system options
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);

        // create the file system wrapper
        return new GSFileSystem(googleHadoopFileSystem, options);
    }

    /**
     * Loads the hadoop configuration, in two steps.
     *
     * <p>1) Find a hadoop conf dir using CoreOptions.FLINK_HADOOP_CONF_DIR or the HADOOP_CONF_DIR
     * environment variable, and load core-default.xml and core-site.xml from that location
     *
     * <p>2) Load hadoop conf from the Flink config, with translations defined above
     *
     * <p>... then merge together, such that keys from the second overwrite the first.
     *
     * @return The Hadoop configuration.
     */
    private static org.apache.hadoop.conf.Configuration getHadoopConfiguration(
            Configuration flinkConfig) {

        // create an empty hadoop configuration
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration();

        // look for a hadoop configuration directory and load configuration from core-default.xml
        // and core-site.xml
        Optional<String> hadoopConfigDir =
                Optional.ofNullable(flinkConfig.get(CoreOptions.FLINK_HADOOP_CONF_DIR));
        if (!hadoopConfigDir.isPresent()) {
            hadoopConfigDir = Optional.ofNullable(System.getenv("HADOOP_CONF_DIR"));
        }
        hadoopConfigDir.ifPresent(
                configDir -> {
                    LOGGER.info("Loading system Hadoop config from {}", configDir);
                    hadoopConfig.addResource(new Path(configDir, "core-default.xml"));
                    hadoopConfig.addResource(new Path(configDir, "core-site.xml"));
                    hadoopConfig.reloadConfiguration();
                });

        // now, load hadoop config from flink and copy key/value pairs into the base config
        HadoopConfigLoader hadoopConfigLoader =
                new HadoopConfigLoader(
                        FLINK_CONFIG_PREFIXES,
                        MIRRORED_CONFIG_KEYS,
                        HADOOP_CONFIG_PREFIX,
                        Collections.emptySet(),
                        Collections.emptySet(),
                        FLINK_SHADING_PREFIX);
        hadoopConfigLoader.setFlinkConfig(flinkConfig);
        org.apache.hadoop.conf.Configuration flinkHadoopConfig =
                hadoopConfigLoader.getOrLoadHadoopConfig();
        for (Map.Entry<String, String> entry : flinkHadoopConfig) {
            hadoopConfig.set(entry.getKey(), entry.getValue());
        }

        return hadoopConfig;
    }

    private String serializeHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig)
            throws RuntimeException {
        try (Writer writer = new StringWriter()) {
            org.apache.hadoop.conf.Configuration.dumpConfiguration(hadoopConfig, writer);
            return writer.toString();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
