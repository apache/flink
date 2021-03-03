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

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/** Delegation token provider implementation for Hadoop FileSystems. */
public class HadoopFSDelegationTokenProvider implements HadoopDelegationTokenProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(HadoopFSDelegationTokenProvider.class);

    private final HadoopDelegationTokenConfiguration hadoopDelegationTokenConf;

    public HadoopFSDelegationTokenProvider(
            HadoopDelegationTokenConfiguration hadoopDelegationTokenConf) {
        this.hadoopDelegationTokenConf = hadoopDelegationTokenConf;
    }

    @Override
    public String serviceName() {
        return "hadoopfs";
    }

    @Override
    public boolean delegationTokensRequired() {
        return UserGroupInformation.isSecurityEnabled();
    }

    @Override
    public Optional<Long> obtainDelegationTokens(Credentials credentials) {
        try {
            Set<FileSystem> fileSystemsToAccess =
                    hadoopDelegationTokenConf.getFileSystemsToAccess();

            final String renewer = getTokenRenewer(hadoopDelegationTokenConf.getHadoopConf());
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
            throw new FlinkRuntimeException("Failed to obtain tokens for Hadoop FileSystems", e);
        }
        // Flink does not support to renew the delegation token currently
        return Optional.empty();
    }

    private String getTokenRenewer(org.apache.hadoop.conf.Configuration hadoopConf) {
        String tokenRenewer = null;
        try {
            tokenRenewer = Master.getMasterPrincipal(hadoopConf);
        } catch (IOException e) {
            LOG.warn("Exception when getting Master principal: {}", e.getMessage());
        }

        LOG.debug("Delegation token renewer is: " + tokenRenewer);
        if (tokenRenewer == null || tokenRenewer.length() == 0) {
            LOG.warn("Can't get Master Kerberos principal for use as renewer.");
        }

        return tokenRenewer;
    }
}
