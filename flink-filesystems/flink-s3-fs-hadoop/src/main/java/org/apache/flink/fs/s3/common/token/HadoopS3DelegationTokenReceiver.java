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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sts.model.Credentials;

import javax.annotation.Nullable;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Delegation token receiver for S3 filesystems using AWS SDK v2.
 *
 * <p>Received credentials are published through {@link
 * AbstractS3DelegationTokenReceiver#updateCredentials(S3SessionCredentials)} so that SDK-agnostic
 * consumers (e.g. the s5cmd integration in {@code FlinkS3FileSystem}) observe the same credentials
 * regardless of which S3 filesystem implementation received them.
 */
@Internal
public abstract class HadoopS3DelegationTokenReceiver implements DelegationTokenReceiver {

    public static final String PROVIDER_CONFIG_NAME = "fs.s3a.aws.credentials.provider";

    /**
     * The SDK v1 based credentials provider that served delegation token credentials before this
     * plugin moved to AWS SDK v2. Users may still reference it from {@code
     * fs.s3a.aws.credentials.provider}; it cannot work here because the SDK v1 interface it
     * implements is not bundled in this plugin, so it is remapped to {@code
     * HadoopDynamicTemporaryAWSCredentialsProvider}. Spelled out as a string literal: referencing
     * the class itself would fail to load it for the same reason.
     */
    private static final String LEGACY_PROVIDER_NAME =
            "org.apache.flink.fs.s3.common.token.DynamicTemporaryAWSCredentialsProvider";

    private static final Logger LOG =
            LoggerFactory.getLogger(HadoopS3DelegationTokenReceiver.class);

    @VisibleForTesting @Nullable static volatile String region;

    public static void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        LOG.info("Updating Hadoop configuration");

        String providers = hadoopConfig.get(PROVIDER_CONFIG_NAME, "");
        String remappedProviders = replaceLegacyProvider(providers);
        if (!remappedProviders.equals(providers)) {
            LOG.info(
                    "Remapped legacy SDK v1 credentials provider {} to {}",
                    LEGACY_PROVIDER_NAME,
                    HadoopDynamicTemporaryAWSCredentialsProvider.NAME);
            providers = remappedProviders;
            hadoopConfig.set(PROVIDER_CONFIG_NAME, providers);
        }
        if (!providers.contains(HadoopDynamicTemporaryAWSCredentialsProvider.NAME)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting provider");
                providers = HadoopDynamicTemporaryAWSCredentialsProvider.NAME;
            } else {
                providers = HadoopDynamicTemporaryAWSCredentialsProvider.NAME + "," + providers;
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

    /**
     * Replaces the legacy SDK v1 delegation token credentials provider with its SDK v2 replacement,
     * dropping duplicates while preserving the order of the remaining chain. Returns the input
     * unchanged when the legacy provider is not referenced.
     */
    private static String replaceLegacyProvider(String providers) {
        boolean legacyFound = false;
        Set<String> remapped = new LinkedHashSet<>();
        for (String provider : providers.split(",")) {
            String trimmed = provider.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (trimmed.equals(LEGACY_PROVIDER_NAME)) {
                legacyFound = true;
                trimmed = HadoopDynamicTemporaryAWSCredentialsProvider.NAME;
            }
            remapped.add(trimmed);
        }
        return legacyFound ? String.join(",", remapped) : providers;
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
            LOG.debug("Region: {}", region);
        }
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        LOG.info("Updating session credentials");
        Credentials sessionCredentials =
                InstantiationUtil.deserializeObject(
                        tokens, HadoopS3DelegationTokenReceiver.class.getClassLoader());
        AbstractS3DelegationTokenReceiver.updateCredentials(
                new S3SessionCredentials(
                        sessionCredentials.accessKeyId(),
                        sessionCredentials.secretAccessKey(),
                        sessionCredentials.sessionToken()));
        LOG.info(
                "Session credentials updated successfully with access key: {} expiration: {}",
                sessionCredentials.accessKeyId(),
                sessionCredentials.expiration());
    }
}
