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

package org.apache.flink.yarn.security;

import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** Hadoop Delegation Token Related Configurations. */
public class HadoopDelegationTokenConfiguration {

    private final Configuration flinkConf;

    private final org.apache.hadoop.conf.Configuration hadoopConf;

    public HadoopDelegationTokenConfiguration(
            Configuration flinkConf, org.apache.hadoop.conf.Configuration hadoopConf) {
        this.flinkConf = flinkConf;
        this.hadoopConf = hadoopConf;
    }

    public org.apache.hadoop.conf.Configuration getHadoopConf() {
        return this.hadoopConf;
    }

    public Set<FileSystem> getFileSystemsToAccess() throws IOException {
        Set<FileSystem> fileSystemsToAccess = new HashSet<>();
        // add default FS
        fileSystemsToAccess.add(FileSystem.get(hadoopConf));

        // add additional FSs
        Set<FileSystem> additionalFileSystems =
                ConfigUtils.decodeListFromConfig(
                                flinkConf, YarnConfigOptions.YARN_ACCESS, Path::new)
                        .stream()
                        .map(
                                FunctionUtils.uncheckedFunction(
                                        path -> path.getFileSystem(hadoopConf)))
                        .collect(Collectors.toSet());
        fileSystemsToAccess.addAll(additionalFileSystems);
        return fileSystemsToAccess;
    }
}
