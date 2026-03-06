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

/**
 * Receiver for AWS S3 delegation tokens that stores credentials for use by the file system.
 *
 * <ul>
 *   <li>{@link #getRegion()} must be called <b>after</b> {@link #init(Configuration)} to get a
 *       non-null region value. Calling it before init will return null.
 *   <li>{@link #getCredentials()} must be called <b>after</b> {@link #onNewTokensObtained(byte[])}
 *       to get valid credentials. Calling it before tokens are obtained will return null.
 *   <li>The volatile modifier ensures visibility across threads, but callers should be prepared to
 *       handle null values if methods are called before their prerequisites.
 *   <li>If multiple calls to {@link #onNewTokensObtained(byte[])} occur concurrently, the final
 *       credential value is non-deterministic, but this is acceptable as tokens are typically
 *       refreshed periodically with sufficient spacing.
 * </ul>
 */
@Internal
public class NativeS3DelegationTokenReceiver implements DelegationTokenReceiver {

    private static final Logger LOG =
            LoggerFactory.getLogger(NativeS3DelegationTokenReceiver.class);

    /**
     * The current session credentials. Will be null until {@link #onNewTokensObtained(byte[])} is
     * called.
     */
    @VisibleForTesting @Nullable static volatile Credentials credentials;

    /**
     * The AWS region. Will be null until {@link #init(Configuration)} is called with a configured
     * region.
     */
    @VisibleForTesting @Nullable static volatile String region;

    @Override
    public String serviceName() {
        return "s3-native";
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
            LOG.debug("Region configured: {}", region);
        }
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        LOG.info("Updating session credentials");
        credentials =
                InstantiationUtil.deserializeObject(
                        tokens, NativeS3DelegationTokenReceiver.class.getClassLoader());
        LOG.info(
                "Session credentials updated successfully with access key: {}, expiration: {}",
                credentials.accessKeyId(),
                credentials.expiration());
    }

    @Nullable
    public static Credentials getCredentials() {
        return credentials;
    }

    @Nullable
    public static String getRegion() {
        return region;
    }
}
