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

package org.apache.flink.fs.gshadoop;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.fs.gshadoop.writer.GSRecoverableOptions;
import org.apache.flink.runtime.util.HadoopConfigLoader;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystemFactory} interface for
 * Google Storage.
 */
public class GSFileSystemFactory implements FileSystemFactory {

    private static final String[] FLINK_CONFIG_PREFIXES = {"gs.", "fs.gs."};

    private static final String[][] MIRRORED_CONFIG_KEYS = {};

    @VisibleForTesting
    public static final String DEFAULT_UPLOAD_CONTENT_TYPE = "application/octet-stream";

    @VisibleForTesting public static final String DEFAULT_UPLOAD_TEMP_PREFIX = ".inprogress";

    private static final ConfigOption<String> UPLOAD_CONTENT_TYPE =
            ConfigOptions.key("gs.upload.content.type")
                    .stringType()
                    .defaultValue(DEFAULT_UPLOAD_CONTENT_TYPE)
                    .withDescription("This option sets content type for uploaded files");

    private static final ConfigOption<String> UPLOAD_TEMP_BUCKET =
            ConfigOptions.key("gs.upload.temp.bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "This option sets the bucket for temporary files for the recoverable writer. If empty, the same bucket of the file to upload is used.");

    private static final ConfigOption<String> UPLOAD_TEMP_PREFIX =
            ConfigOptions.key("gs.upload.temp.prefix")
                    .stringType()
                    .defaultValue(DEFAULT_UPLOAD_TEMP_PREFIX)
                    .withDescription(
                            "This option sets the prefix for temporary files for the recoverable writer");

    private static final ConfigOption<Integer> UPLOAD_CHUNK_SIZE =
            ConfigOptions.key("gs.upload.chunk.size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "This option sets a custom chunk size for resumable uploads. A zero value (the default) means the chunk size will not be set and Google's default value will be used.");

    private static HadoopConfigLoader createDefaultHadoopConfigLoader() {
        return new HadoopConfigLoader(
                FLINK_CONFIG_PREFIXES,
                MIRRORED_CONFIG_KEYS,
                "fs.gs.",
                Collections.emptySet(),
                Collections.emptySet(),
                "");
    }

    private final GSFileSystemHelper fileSystemHelper;

    private final HadoopConfigLoader hadoopConfigLoader;

    private Configuration flinkConfig;

    /**
     * Constructs a GSFileSystemFactory.
     *
     * @param fileSystemHelper The {@link org.apache.flink.fs.gshadoop.GSFileSystemHelper} instance
     * @param hadoopConfigLoader The hadoop configuration loader
     */
    private GSFileSystemFactory(
            GSFileSystemHelper fileSystemHelper, HadoopConfigLoader hadoopConfigLoader) {
        this.fileSystemHelper = Preconditions.checkNotNull(fileSystemHelper);
        this.hadoopConfigLoader = Preconditions.checkNotNull(hadoopConfigLoader);

        // assign an empty flink config so that we have an empty configuration in the event that
        // the configure method (below) is not called for whatever reason
        this.flinkConfig = new Configuration();
    }

    @VisibleForTesting
    GSFileSystemFactory(GSFileSystemHelper fileSystemHelper) {
        this(fileSystemHelper, createDefaultHadoopConfigLoader());
    }

    /** Constructs the factory using the default helper. */
    public GSFileSystemFactory() {
        this(new DefaultGSFileSystemHelper());
    }

    @Override
    public String getScheme() {
        return "gs";
    }

    @Override
    public void configure(Configuration config) {
        Preconditions.checkNotNull(config);

        flinkConfig = config;
        hadoopConfigLoader.setFlinkConfig(config);
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        Preconditions.checkNotNull(fsUri);

        // configure the hadoop filesystem
        org.apache.hadoop.conf.Configuration hadoopConfig =
                hadoopConfigLoader.getOrLoadHadoopConfig();
        org.apache.hadoop.fs.FileSystem hadoopFileSystem =
                fileSystemHelper.createHadoopFileSystem();
        hadoopFileSystem.initialize(fsUri, hadoopConfig);

        // read options and construct options instance
        GSRecoverableOptions options =
                new GSRecoverableOptions(
                        fileSystemHelper.getRecoverableWriterHelper(),
                        flinkConfig.getString(UPLOAD_CONTENT_TYPE),
                        flinkConfig.getString(UPLOAD_TEMP_BUCKET),
                        flinkConfig.getString(UPLOAD_TEMP_PREFIX),
                        flinkConfig.getInteger(UPLOAD_CHUNK_SIZE));

        return new org.apache.flink.fs.gshadoop.GSFileSystem(hadoopFileSystem, options);
    }
}
