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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.util.HadoopConfigLoader;
import org.apache.flink.util.Preconditions;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystemFactory} interface for
 * Google Storage.
 */
public class GSFileSystemFactory implements FileSystemFactory {

    private static final String SCHEME = "gs";

    private static final String HADOOP_CONFIG_PREFIX = "fs.gs.";

    private static final String[] FLINK_CONFIG_PREFIXES = {"gs.", HADOOP_CONFIG_PREFIX};

    private static final String[][] MIRRORED_CONFIG_KEYS = {};

    private static final String FLINK_SHADING_PREFIX = "";

    public static final ConfigOption<String> WRITER_TEMPORARY_BUCKET_NAME =
            ConfigOptions.key("gs.writer.temporary.bucket.name")
                    .stringType()
                    .defaultValue(GSFileSystemOptions.DEFAULT_WRITER_TEMPORARY_BUCKET_NAME)
                    .withDescription(
                            "This option sets the bucket name used by the recoverable writer to store temporary files. "
                                    + "If empty, temporary files are stored in the same bucket as the final file being written.");

    public static final ConfigOption<String> WRITER_TEMPORARY_OBJECT_PREFIX =
            ConfigOptions.key("gs.writer.temporary.object.prefix")
                    .stringType()
                    .defaultValue(GSFileSystemOptions.DEFAULT_WRITER_TEMPORARY_OBJECT_PREFIX)
                    .withDescription(
                            "This option sets the prefix used by the recoverable writer when writing temporary files. This prefix is applied to the "
                                    + "final object name to form the base name for temporary files.");

    public static final ConfigOption<String> WRITER_CONTENT_TYPE =
            ConfigOptions.key("gs.writer.content.type")
                    .stringType()
                    .defaultValue(GSFileSystemOptions.DEFAULT_WRITER_CONTENT_TYPE)
                    .withDescription(
                            "This option sets the content type applied to files written by the recoverable writer.");

    public static final ConfigOption<Integer> WRITER_CHUNK_SIZE =
            ConfigOptions.key("gs.writer.chunk.size")
                    .intType()
                    .defaultValue(GSFileSystemOptions.DEFAULT_WRITER_CHUNK_SIZE)
                    .withDescription(
                            "This option sets the chunk size for writes by the recoverable writer. This value is passed through to the underlying "
                                    + "Google WriteChannel; if zero, the default WriteChannel value is used.");

    private final HadoopConfigLoader hadoopConfigLoader;

    private Configuration flinkConfig;

    /** Constructs the Google Storage file system factory. */
    public GSFileSystemFactory() {
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
        Preconditions.checkNotNull(flinkConfig);

        this.flinkConfig = flinkConfig;
        hadoopConfigLoader.setFlinkConfig(flinkConfig);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        Preconditions.checkNotNull(fsUri);

        // create and configure the Google Hadoop file system
        org.apache.hadoop.conf.Configuration hadoopConfig =
                hadoopConfigLoader.getOrLoadHadoopConfig();
        GoogleHadoopFileSystem googleHadoopFileSystem = new GoogleHadoopFileSystem();
        googleHadoopFileSystem.initialize(fsUri, hadoopConfig);

        // construct the file system options
        String writerTemporaryBucketName = flinkConfig.getString(WRITER_TEMPORARY_BUCKET_NAME);
        String writerTemporaryObjectPrefix = flinkConfig.getString(WRITER_TEMPORARY_OBJECT_PREFIX);
        String writerContentType = flinkConfig.getString(WRITER_CONTENT_TYPE);
        int writerChunkSize = flinkConfig.getInteger(WRITER_CHUNK_SIZE);
        GSFileSystemOptions options =
                new GSFileSystemOptions(
                        writerTemporaryBucketName,
                        writerTemporaryObjectPrefix,
                        writerContentType,
                        writerChunkSize);

        // create the file system wrapper
        return new GSFileSystem(googleHadoopFileSystem, options);
    }
}
