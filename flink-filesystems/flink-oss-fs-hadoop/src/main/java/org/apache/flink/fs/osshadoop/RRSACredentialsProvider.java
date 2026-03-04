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

package org.apache.flink.fs.osshadoop;

import org.apache.flink.annotation.Internal;

import com.aliyun.credentials.models.CredentialModel;
import com.aliyun.credentials.provider.OIDCRoleArnCredentialProvider;
import com.aliyun.oss.common.auth.BasicCredentials;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.InvalidCredentialsException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * RRSA (RAM Roles for Service Accounts) credentials provider for OSS.
 *
 * <p>This provider enables Flink applications running in Alibaba Cloud Kubernetes (ACK) to
 * authenticate with OSS using OIDC-based service account credentials. RRSA eliminates the need for
 * hard-coded access keys by allowing pods to assume specific RAM roles.
 *
 * <p>RRSA requires the following environment variables to be set (automatically injected by ACK):
 *
 * <ul>
 *   <li>ALIBABA_CLOUD_OIDC_PROVIDER_ARN - The OIDC provider ARN
 *   <li>ALIBABA_CLOUD_ROLE_ARN - The RAM role ARN to assume
 *   <li>ALIBABA_CLOUD_OIDC_TOKEN_FILE - Path to the OIDC token file (service account token)
 * </ul>
 *
 * <p>For more information, see: <a
 * href="https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/use-rrsa-to-authorize-pods-to-access-different-cloud-services">RRSA
 * Documentation</a>
 */
@Internal
public class RRSACredentialsProvider implements CredentialsProvider {

    private static final Logger LOG = LoggerFactory.getLogger(RRSACredentialsProvider.class);

    public static final String OIDC_PROVIDER_ARN_ENV = "ALIBABA_CLOUD_OIDC_PROVIDER_ARN";
    public static final String ROLE_ARN_ENV = "ALIBABA_CLOUD_ROLE_ARN";
    public static final String OIDC_TOKEN_FILE_ENV = "ALIBABA_CLOUD_OIDC_TOKEN_FILE";

    @Nullable private OIDCRoleArnCredentialProvider oidcProvider;

    /** Default constructor required by Hadoop's credential provider mechanism. */
    public RRSACredentialsProvider() {}

    /**
     * Constructor that accepts Hadoop configuration. Required by some Hadoop filesystem
     * implementations.
     */
    public RRSACredentialsProvider(Configuration conf) {
        // Configuration not used for RRSA as it relies on environment variables
    }

    /**
     * Checks if RRSA environment variables are present.
     *
     * @return true if all required RRSA environment variables are set
     */
    public static boolean isRrsaEnvironmentAvailable() {
        String oidcProviderArn = System.getenv(OIDC_PROVIDER_ARN_ENV);
        String roleArn = System.getenv(ROLE_ARN_ENV);
        String oidcTokenFile = System.getenv(OIDC_TOKEN_FILE_ENV);

        boolean available =
                !isNullOrEmpty(oidcProviderArn)
                        && !isNullOrEmpty(roleArn)
                        && !isNullOrEmpty(oidcTokenFile);

        if (available) {
            LOG.info(
                    "RRSA environment detected: OIDC Provider ARN={}, Role ARN={}, Token File={}",
                    oidcProviderArn,
                    roleArn,
                    oidcTokenFile);
        }

        return available;
    }

    @Override
    public void setCredentials(Credentials credentials) {
        // Not used for RRSA - credentials are obtained from OIDC provider
    }

    @Override
    public Credentials getCredentials() {
        try {
            // Lazy initialization of OIDC provider
            if (oidcProvider == null) {
                synchronized (this) {
                    if (oidcProvider == null) {
                        if (!isRrsaEnvironmentAvailable()) {
                            throw new InvalidCredentialsException(
                                    "RRSA environment variables are not available");
                        }
                        LOG.info("Initializing RRSA OIDC credential provider");
                        oidcProvider = OIDCRoleArnCredentialProvider.builder().build();
                    }
                }
            }

            // Get credentials from OIDC provider (automatically cached and refreshed)
            LOG.debug("Fetching credentials using RRSA");
            CredentialModel credModel = oidcProvider.getCredentials();

            // Convert to OSS BasicCredentials
            long expirationSeconds = 0;
            if (credModel.getExpiration() > 0) {
                expirationSeconds = (credModel.getExpiration() - System.currentTimeMillis()) / 1000;
                LOG.debug("RRSA credentials obtained, expires in {} seconds", expirationSeconds);
            }

            return new BasicCredentials(
                    credModel.getAccessKeyId(),
                    credModel.getAccessKeySecret(),
                    credModel.getSecurityToken(),
                    expirationSeconds);

        } catch (Exception e) {
            LOG.error("Failed to get RRSA credentials", e);
            throw new InvalidCredentialsException(
                    "Failed to get RRSA credentials: " + e.getMessage(), e);
        }
    }

    private static boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }
}
