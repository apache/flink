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

package org.apache.flink.fs.coshadoop;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import org.apache.hadoop.fs.cosn.CosNFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/** Simple factory for the COSN file system. */
public class COSNFileSystemFactory implements FileSystemFactory {
    private static final Logger LOG = LoggerFactory.getLogger(COSNFileSystemFactory.class);

    private Configuration flinkConfig;

    private org.apache.hadoop.conf.Configuration hadoopConfig;

    private static final String[] FLINK_CONFIG_PREFIXES = {"fs.cosn."};
    private static final String FLINK_ABSTRACT_FILESYSTEM_COSN_IMPL =
            "fs.cosn.AbstractFileSystem.cosn.impl";

    @Override
    public String getScheme() {
        return "cosn";
    }

    @Override
    public void configure(Configuration config) {
        flinkConfig = config;
        hadoopConfig = null;
    }

    @Override
    public FileSystem create(URI fsUri) throws IOException {
        this.hadoopConfig = getHadoopConfiguration();

        final String scheme = fsUri.getScheme();
        final String authority = fsUri.getAuthority();

        if (scheme == null && authority == null) {
            fsUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
        } else if (scheme != null && authority == null) {
            URI defaultUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
            if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
                fsUri = defaultUri;
            }
        }

        final CosNFileSystem fs = new CosNFileSystem();
        fs.initialize(fsUri, hadoopConfig);
        return new HadoopFileSystem(fs);
    }

    @VisibleForTesting
    org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        if (flinkConfig == null) {
            return conf;
        }

        // read all configuration with prefix 'FLINK_CONFIG_PREFIXES'
        for (String key : flinkConfig.keySet()) {
            for (String prefix : FLINK_CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    if (FLINK_ABSTRACT_FILESYSTEM_COSN_IMPL.equals(key)) {
                        conf.set(
                                "fs.AbstractFileSystem.cosn.impl",
                                flinkConfig.getString(key, null));
                    }

                    String value = flinkConfig.getString(key, null);
                    conf.set(key, value);

                    LOG.debug(
                            "Adding Flink config entry for {} as {} to Hadoop config",
                            key,
                            conf.get(key));
                }
            }
        }
        return conf;
    }
}
