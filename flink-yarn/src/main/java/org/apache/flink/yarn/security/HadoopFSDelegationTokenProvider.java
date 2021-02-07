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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Delegation token provider implementation for Hadoop FileSystems. */
public class HadoopFSDelegationTokenProvider implements HadoopDelegationTokenProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(HadoopFSDelegationTokenProvider.class);

    @Override
    public String serviceName() {
        return "hadoopfs";
    }

    @Override
    public boolean delegationTokensRequired(
            Configuration flinkConf, org.apache.hadoop.conf.Configuration hadoopConf) {
        return UserGroupInformation.isSecurityEnabled();
    }

    @Override
    public Optional<Long> obtainDelegationTokens(
            Configuration flinkConf,
            org.apache.hadoop.conf.Configuration hadoopConf,
            Credentials credentials) {
        try {
            Set<FileSystem> fileSystemsToAccess = getFileSystemsToAccess(flinkConf, hadoopConf);

            final String renewer = getTokenRenewer(hadoopConf);
            fileSystemsToAccess.forEach(
                    fs -> {
                        try {
                            LOG.info("Getting FS token for: {} with renewer {}", fs, renewer);
                            fs.addDelegationTokens(renewer, credentials);
                        } catch (IOException e) {
                            LOG.warn("Failed to get token for {}.", fs);
                        }
                    });
        } catch (IOException e) {
            LOG.error("Failed to obtain tokens for Hadoop FileSystems: {}", e.getMessage());
        }
        // Flink does not support to renew the delegation token currently
        return Optional.empty();
    }

    @VisibleForTesting
    Set<FileSystem> getFileSystemsToAccess(
            Configuration flinkConf, org.apache.hadoop.conf.Configuration hadoopConf)
            throws IOException {
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

    private String getTokenRenewer(org.apache.hadoop.conf.Configuration hadoopConf)
            throws IOException {
        String tokenRenewer = Master.getMasterPrincipal(hadoopConf);
        LOG.debug("Delegation token renewer is: " + tokenRenewer);

        if (tokenRenewer == null || tokenRenewer.length() == 0) {
            String errorMessage = "Can't get Master Kerberos principal for use as renewer.";
            LOG.error(errorMessage);
            throw new IOException(errorMessage);
        }

        return tokenRenewer;
    }
}
