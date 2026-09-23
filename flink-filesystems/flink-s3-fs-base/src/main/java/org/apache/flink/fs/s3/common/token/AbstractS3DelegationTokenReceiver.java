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

import javax.annotation.Nullable;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Delegation token receiver for S3 filesystems.
 *
 * <p>This class deliberately references no AWS SDK types (see {@link S3SessionCredentials}), so it
 * can be loaded both in the {@code flink-s3-fs-presto} plugin (AWS SDK v1 only) and the {@code
 * flink-s3-fs-hadoop} plugin (AWS SDK v2 only). The received credentials are exposed through {@link
 * #getCredentials()} to the SDK-specific credential providers and to SDK-agnostic consumers such as
 * the s5cmd integration in {@code FlinkS3FileSystem}.
 */
@Internal
public abstract class AbstractS3DelegationTokenReceiver implements DelegationTokenReceiver {

    public static final String PROVIDER_CONFIG_NAME = "fs.s3a.aws.credentials.provider";

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractS3DelegationTokenReceiver.class);

    @VisibleForTesting @Nullable static volatile S3SessionCredentials credentials;

    @VisibleForTesting @Nullable static volatile String region;

    public static void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        updateHadoopConfig(hadoopConfig, DynamicTemporaryAWSCredentialsProvider.NAME);
    }

    /**
     * Registers the given delegation token credentials provider in {@code
     * fs.s3a.aws.credentials.provider} and propagates the configured region.
     *
     * <p>When {@code credentialsProviderName} differs from the SDK v1 based {@link
     * DynamicTemporaryAWSCredentialsProvider}, any user-configured reference to that legacy
     * provider is remapped to {@code credentialsProviderName}: the legacy provider cannot be loaded
     * in plugins that do not bundle AWS SDK v1 (e.g. {@code flink-s3-fs-hadoop}).
     */
    public static void updateHadoopConfig(
            org.apache.hadoop.conf.Configuration hadoopConfig, String credentialsProviderName) {
        LOG.info("Updating Hadoop configuration");

        String providers = hadoopConfig.get(PROVIDER_CONFIG_NAME, "");
        if (!credentialsProviderName.equals(DynamicTemporaryAWSCredentialsProvider.NAME)) {
            String remappedProviders = replaceLegacyProvider(providers, credentialsProviderName);
            if (!remappedProviders.equals(providers)) {
                LOG.info(
                        "Remapped legacy SDK v1 credentials provider {} to {}",
                        DynamicTemporaryAWSCredentialsProvider.NAME,
                        credentialsProviderName);
                providers = remappedProviders;
                hadoopConfig.set(PROVIDER_CONFIG_NAME, providers);
            }
        }
        if (!providers.contains(credentialsProviderName)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting provider");
                providers = credentialsProviderName;
            } else {
                providers = credentialsProviderName + "," + providers;
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
     * Replaces the legacy SDK v1 delegation token credentials provider with its replacement,
     * dropping duplicates while preserving the order of the remaining chain. Returns the input
     * unchanged when the legacy provider is not referenced.
     */
    private static String replaceLegacyProvider(String providers, String replacement) {
        boolean legacyFound = false;
        Set<String> remapped = new LinkedHashSet<>();
        for (String provider : providers.split(",")) {
            String trimmed = provider.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (trimmed.equals(DynamicTemporaryAWSCredentialsProvider.NAME)) {
                legacyFound = true;
                trimmed = replacement;
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
        S3SessionCredentials newCredentials =
                InstantiationUtil.deserializeObject(
                        tokens, AbstractS3DelegationTokenReceiver.class.getClassLoader());
        credentials = newCredentials;
        LOG.info(
                "Session credentials updated successfully with access key: {} expiration: {}",
                newCredentials.getAccessKeyId(),
                newCredentials.getExpirationEpochMilli());
    }

    @Nullable
    public static S3SessionCredentials getCredentials() {
        return credentials;
    }
}
