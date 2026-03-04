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

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RRSACredentialsProvider}. */
class RRSACredentialsProviderTest {

    @Test
    @ClearEnvironmentVariable(key = RRSACredentialsProvider.OIDC_PROVIDER_ARN_ENV)
    @ClearEnvironmentVariable(key = RRSACredentialsProvider.ROLE_ARN_ENV)
    @ClearEnvironmentVariable(key = RRSACredentialsProvider.OIDC_TOKEN_FILE_ENV)
    void testIsRrsaEnvironmentNotAvailable() {
        // Verify that RRSA is not available when environment variables are not set
        assertThat(RRSACredentialsProvider.isRrsaEnvironmentAvailable())
                .as("RRSA should not be available without environment variables")
                .isFalse();
    }

    @Test
    @SetEnvironmentVariable(
            key = RRSACredentialsProvider.OIDC_PROVIDER_ARN_ENV,
            value = "acs:ram::123456789:oidc-provider/ack-rrsa-test")
    @ClearEnvironmentVariable(key = RRSACredentialsProvider.ROLE_ARN_ENV)
    @ClearEnvironmentVariable(key = RRSACredentialsProvider.OIDC_TOKEN_FILE_ENV)
    void testIsRrsaEnvironmentPartiallyAvailableOidcProvider() {
        // Verify that RRSA is not available when only OIDC provider ARN is set
        assertThat(RRSACredentialsProvider.isRrsaEnvironmentAvailable())
                .as("RRSA should not be available with only OIDC provider ARN")
                .isFalse();
    }

    @Test
    @SetEnvironmentVariable(
            key = RRSACredentialsProvider.OIDC_PROVIDER_ARN_ENV,
            value = "acs:ram::123456789:oidc-provider/ack-rrsa-test")
    @SetEnvironmentVariable(
            key = RRSACredentialsProvider.ROLE_ARN_ENV,
            value = "acs:ram::123456789:role/test-rrsa-role")
    @ClearEnvironmentVariable(key = RRSACredentialsProvider.OIDC_TOKEN_FILE_ENV)
    void testIsRrsaEnvironmentPartiallyAvailableWithoutTokenFile() {
        // Verify that RRSA is not available when token file is missing
        assertThat(RRSACredentialsProvider.isRrsaEnvironmentAvailable())
                .as("RRSA should not be available without OIDC token file")
                .isFalse();
    }

    @Test
    @SetEnvironmentVariable(
            key = RRSACredentialsProvider.OIDC_PROVIDER_ARN_ENV,
            value = "acs:ram::123456789:oidc-provider/ack-rrsa-test")
    @SetEnvironmentVariable(
            key = RRSACredentialsProvider.ROLE_ARN_ENV,
            value = "acs:ram::123456789:role/test-rrsa-role")
    @SetEnvironmentVariable(
            key = RRSACredentialsProvider.OIDC_TOKEN_FILE_ENV,
            value = "/tmp/oidc-token")
    void testIsRrsaEnvironmentAvailable() {
        // Verify that RRSA is available when all environment variables are set
        assertThat(RRSACredentialsProvider.isRrsaEnvironmentAvailable())
                .as("RRSA should be available with all environment variables set")
                .isTrue();
    }

    @Test
    @SetEnvironmentVariable(
            key = RRSACredentialsProvider.OIDC_PROVIDER_ARN_ENV,
            value = "acs:ram::123456789:oidc-provider/ack-rrsa-test")
    @SetEnvironmentVariable(
            key = RRSACredentialsProvider.ROLE_ARN_ENV,
            value = "acs:ram::123456789:role/test-rrsa-role")
    @SetEnvironmentVariable(
            key = RRSACredentialsProvider.OIDC_TOKEN_FILE_ENV,
            value = "/tmp/oidc-token")
    void testGetCredentialsWithMockEnvironment() {
        // Create provider with RRSA environment variables set
        RRSACredentialsProvider provider = new RRSACredentialsProvider();

        // With fake credentials, getCredentials should fail when trying to actually fetch tokens
        // This is expected behavior - we're just testing that the provider is initialized
        // correctly
        assertThatThrownBy(provider::getCredentials)
                .as("Getting credentials with fake RRSA environment should fail")
                .hasMessageContaining("Failed to get RRSA credentials");
    }

    @Test
    @ClearEnvironmentVariable(key = RRSACredentialsProvider.OIDC_PROVIDER_ARN_ENV)
    @ClearEnvironmentVariable(key = RRSACredentialsProvider.ROLE_ARN_ENV)
    @ClearEnvironmentVariable(key = RRSACredentialsProvider.OIDC_TOKEN_FILE_ENV)
    void testGetCredentialsWithoutEnvironment() {
        // Create provider without RRSA environment variables
        RRSACredentialsProvider provider = new RRSACredentialsProvider();

        // Should fail immediately when environment is not available
        assertThatThrownBy(provider::getCredentials)
                .as("Getting credentials without RRSA environment should fail")
                .hasMessageContaining("RRSA environment variables are not available");
    }

    @Test
    void testSetCredentialsIsNoOp() {
        // setCredentials should be a no-op for RRSA provider
        RRSACredentialsProvider provider = new RRSACredentialsProvider();
        provider.setCredentials(null); // Should not throw
    }
}
