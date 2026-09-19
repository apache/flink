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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Delegation token provider for S3 filesystems.
 *
 * <p>This class deliberately references no AWS SDK types (see {@link S3SessionCredentials}): the
 * STS call to obtain session credentials is left to {@link #getSessionCredentials(String, String,
 * String)}, implemented with AWS SDK v1 in {@code flink-s3-fs-presto} and with AWS SDK v2 in {@code
 * flink-s3-fs-hadoop}.
 */
@Internal
public abstract class AbstractS3DelegationTokenProvider implements DelegationTokenProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractS3DelegationTokenProvider.class);

    private String region;
    private String accessKey;
    private String secretKey;

    @Override
    public void init(Configuration configuration) {
        region = configuration.getString(String.format("%s.region", serviceConfigPrefix()), null);
        if (!StringUtils.isNullOrWhitespaceOnly(region)) {
            LOG.debug("Region: {}", region);
        }

        accessKey =
                configuration.getString(
                        String.format("%s.access-key", serviceConfigPrefix()), null);
        if (!StringUtils.isNullOrWhitespaceOnly(accessKey)) {
            LOG.debug("Access key: {}", accessKey);
        }

        secretKey =
                configuration.getString(
                        String.format("%s.secret-key", serviceConfigPrefix()), null);
        if (!StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            LOG.debug("Secret key: {} (sensitive information)", GlobalConfiguration.HIDDEN_CONTENT);
        }
    }

    @Override
    public boolean delegationTokensRequired() {
        if (StringUtils.isNullOrWhitespaceOnly(region)
                || StringUtils.isNullOrWhitespaceOnly(accessKey)
                || StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            LOG.debug("Not obtaining session credentials because not all configurations are set");
            return false;
        }
        return true;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        LOG.info("Obtaining session credentials token with access key: {}", accessKey);

        S3SessionCredentials credentials = getSessionCredentials(region, accessKey, secretKey);
        LOG.info(
                "Session credentials obtained successfully with access key: {} expiration: {}",
                credentials.getAccessKeyId(),
                credentials.getExpirationEpochMilli());

        return new ObtainedDelegationTokens(
                InstantiationUtil.serializeObject(credentials),
                Optional.of(credentials.getExpirationEpochMilli()));
    }

    /**
     * Obtains session credentials from AWS STS with the SDK bundled into the concrete filesystem
     * plugin.
     */
    protected abstract S3SessionCredentials getSessionCredentials(
            String region, String accessKey, String secretKey) throws Exception;
}
