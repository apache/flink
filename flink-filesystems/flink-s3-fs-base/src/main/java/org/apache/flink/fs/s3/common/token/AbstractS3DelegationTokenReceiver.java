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

package org.apache.flink.fs.s3.common.token;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import com.amazonaws.services.securitytoken.model.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** Delegation token receiver for S3 filesystems. */
@Internal
public abstract class AbstractS3DelegationTokenReceiver implements DelegationTokenReceiver {

    public static final String PROVIDER_CONFIG_NAME = "fs.s3a.aws.credentials.provider";

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractS3DelegationTokenReceiver.class);

    @VisibleForTesting @Nullable static volatile Credentials credentials;

    @VisibleForTesting @Nullable static volatile String region;

    public static void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        LOG.info("Updating Hadoop configuration");

        String providers = hadoopConfig.get(PROVIDER_CONFIG_NAME, "");
        if (!providers.contains(DynamicTemporaryAWSCredentialsProvider.NAME)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting provider");
                providers = DynamicTemporaryAWSCredentialsProvider.NAME;
            } else {
                providers = DynamicTemporaryAWSCredentialsProvider.NAME + "," + providers;
                LOG.debug("Prepending provider, new providers value: {}", providers);
            }
            hadoopConfig.set(PROVIDER_CONFIG_NAME, providers);
        } else {
            LOG.debug("Provider already exists");
        }

        if (!StringUtils.isNullOrWhitespaceOnly(region)) {
            LOG.debug("Setting region");
            hadoopConfig.set("fs.s3a.endpoint.region", region);
        }

        LOG.info("Updated Hadoop configuration successfully");
    }

    @Override
    public void init(Configuration configuration) {
        region =
                configuration.getString(
                        String.format(
                                "%s.%s.region",
                                DelegationTokenProvider.CONFIG_PREFIX, serviceName()),
                        null);
        if (!StringUtils.isNullOrWhitespaceOnly(region)) {
            LOG.debug("Region: " + region);
        }
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        LOG.info("Updating session credentials");
        credentials =
                InstantiationUtil.deserializeObject(
                        tokens, AbstractS3DelegationTokenReceiver.class.getClassLoader());
        LOG.info(
                "Session credentials updated successfully with access key: {} expiration: {}",
                credentials.getAccessKeyId(),
                credentials.getExpiration());
    }

    @Nullable
    public static Credentials getCredentials() {
        return credentials;
    }
}
