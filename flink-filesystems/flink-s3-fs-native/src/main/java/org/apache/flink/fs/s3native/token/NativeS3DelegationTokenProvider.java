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

package org.apache.flink.fs.s3native.token;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.GetSessionTokenRequest;
import software.amazon.awssdk.services.sts.model.GetSessionTokenResponse;

import java.util.Optional;

/** Provider for AWS S3 delegation tokens using STS session credentials. */
@Internal
public class NativeS3DelegationTokenProvider implements DelegationTokenProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(NativeS3DelegationTokenProvider.class);

    /**
     * Pattern for validating AWS region format. Regions follow the pattern: {area}-{sub-area}-{N}
     * Examples: us-east-1, eu-west-2, ap-southeast-1. Also allows special regions like us-gov-*,
     * cn-*, and local/fips variants.
     */
    private static final java.util.regex.Pattern REGION_PATTERN =
            java.util.regex.Pattern.compile("^[a-z]{2}(-gov)?-[a-z]+-\\d+(-fips)?$|^local$");

    private volatile String region;
    private volatile String accessKey;
    private volatile String secretKey;

    /**
     * Using unique service name. Avoids conflict with s3-fs-hadoop plugin. Both plugins can coexist
     * with different service names
     *
     * @return ServiceName
     */
    @Override
    public String serviceName() {
        return "s3-native";
    }

    @Override
    public void init(Configuration configuration) {
        String configPrefix = String.format("%s.%s", CONFIG_PREFIX, serviceName());

        region = configuration.getString(configPrefix + ".region", null);
        if (!StringUtils.isNullOrWhitespaceOnly(region)) {
            if (!REGION_PATTERN.matcher(region).matches()) {
                LOG.warn(
                        "Region '{}' does not match expected AWS region format. "
                                + "Expected format: {area}-{sub-area}-{N} (e.g., us-east-1, eu-west-2)",
                        region);
            }
            LOG.debug("Region: {}", region);
        }

        accessKey = configuration.getString(configPrefix + ".access-key", null);
        if (!StringUtils.isNullOrWhitespaceOnly(accessKey)) {
            LOG.debug("Access key configured");
        }

        secretKey = configuration.getString(configPrefix + ".secret-key", null);
        if (!StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            LOG.debug("Secret key: {} (sensitive information)", GlobalConfiguration.HIDDEN_CONTENT);
        }
    }

    @Override
    public boolean delegationTokensRequired() {
        if (StringUtils.isNullOrWhitespaceOnly(region)
                || StringUtils.isNullOrWhitespaceOnly(accessKey)
                || StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            return false;
        }
        return true;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        LOG.info("Obtaining session credentials token with access key: {}", accessKey);
        StsClient stsClient =
                StsClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(accessKey, secretKey)))
                        .build();
        try {
            GetSessionTokenRequest request = GetSessionTokenRequest.builder().build();
            GetSessionTokenResponse response = stsClient.getSessionToken(request);
            Credentials credentials = response.credentials();

            LOG.info(
                    "Session credentials obtained successfully with access key: {}, expiration: {}",
                    credentials.accessKeyId(),
                    credentials.expiration());
            return new ObtainedDelegationTokens(
                    InstantiationUtil.serializeObject(credentials),
                    Optional.of(credentials.expiration().toEpochMilli()));
        } finally {
            stsClient.close();
        }
    }
}
