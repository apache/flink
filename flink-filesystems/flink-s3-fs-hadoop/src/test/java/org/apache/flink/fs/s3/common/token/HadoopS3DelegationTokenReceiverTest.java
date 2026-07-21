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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;

import static org.apache.flink.fs.s3.common.token.HadoopS3DelegationTokenReceiver.PROVIDER_CONFIG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HadoopS3DelegationTokenReceiver}. */
class HadoopS3DelegationTokenReceiverTest {

    /**
     * The SDK v1 based provider that served delegation token credentials before this plugin moved
     * to AWS SDK v2. Spelled out as a literal on purpose: existing user configurations contain
     * exactly this string.
     */
    private static final String LEGACY_PROVIDER =
            "org.apache.flink.fs.s3.common.token.DynamicTemporaryAWSCredentialsProvider";

    private static final String CUSTOM_PROVIDER = "com.example.CustomCredentialsProvider";

    @BeforeEach
    void beforeEach() {
        HadoopS3DelegationTokenReceiver.region = null;
    }

    @AfterEach
    void afterEach() {
        HadoopS3DelegationTokenReceiver.region = null;
    }

    @Test
    void updateHadoopConfigShouldInjectProviderWhenNoneConfigured() {
        Configuration hadoopConfig = new Configuration(false);

        HadoopS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);

        assertThat(hadoopConfig.get(PROVIDER_CONFIG_NAME))
                .isEqualTo(HadoopDynamicTemporaryAWSCredentialsProvider.NAME);
    }

    @Test
    void updateHadoopConfigShouldPrependProviderToExistingChain() {
        Configuration hadoopConfig = new Configuration(false);
        hadoopConfig.set(PROVIDER_CONFIG_NAME, CUSTOM_PROVIDER);

        HadoopS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);

        assertThat(hadoopConfig.get(PROVIDER_CONFIG_NAME))
                .isEqualTo(
                        HadoopDynamicTemporaryAWSCredentialsProvider.NAME + "," + CUSTOM_PROVIDER);
    }

    @Test
    void updateHadoopConfigShouldNotDuplicateProvider() {
        Configuration hadoopConfig = new Configuration(false);
        hadoopConfig.set(PROVIDER_CONFIG_NAME, HadoopDynamicTemporaryAWSCredentialsProvider.NAME);

        HadoopS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);

        assertThat(hadoopConfig.get(PROVIDER_CONFIG_NAME))
                .isEqualTo(HadoopDynamicTemporaryAWSCredentialsProvider.NAME);
    }

    @Test
    void updateHadoopConfigShouldRemapLegacySdkV1ProviderName() {
        Configuration hadoopConfig = new Configuration(false);
        hadoopConfig.set(PROVIDER_CONFIG_NAME, LEGACY_PROVIDER);

        HadoopS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);

        assertThat(hadoopConfig.get(PROVIDER_CONFIG_NAME))
                .isEqualTo(HadoopDynamicTemporaryAWSCredentialsProvider.NAME);
    }

    @Test
    void updateHadoopConfigShouldRemapLegacyNameInPlaceWithinProviderChain() {
        Configuration hadoopConfig = new Configuration(false);
        hadoopConfig.set(
                PROVIDER_CONFIG_NAME,
                "com.example.First, " + LEGACY_PROVIDER + " ,com.example.Last");

        HadoopS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);

        assertThat(hadoopConfig.get(PROVIDER_CONFIG_NAME))
                .isEqualTo(
                        "com.example.First,"
                                + HadoopDynamicTemporaryAWSCredentialsProvider.NAME
                                + ",com.example.Last");
    }

    @Test
    void updateHadoopConfigShouldDropLegacyNameWhenSdkV2ProviderAlreadyConfigured() {
        Configuration hadoopConfig = new Configuration(false);
        hadoopConfig.set(
                PROVIDER_CONFIG_NAME,
                HadoopDynamicTemporaryAWSCredentialsProvider.NAME + "," + LEGACY_PROVIDER);

        HadoopS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);

        assertThat(hadoopConfig.get(PROVIDER_CONFIG_NAME))
                .isEqualTo(HadoopDynamicTemporaryAWSCredentialsProvider.NAME);
    }

    /**
     * End-to-end regression for the legacy provider name: hadoop-aws must be able to build the
     * credential provider chain from a configuration that referenced the SDK v1 provider, and the
     * chain must not reference the legacy class at all. In the shaded plugin jar instantiating the
     * legacy class fails with {@code NoClassDefFoundError:
     * com/amazonaws/auth/AWSCredentialsProvider} (the SDK v1 interface is not bundled); on the
     * unshaded test classpath it would load, so the decisive assertion is its absence from the
     * resolved chain.
     */
    @Test
    void remappedLegacyProviderShouldResolveToWorkingProviderChain() throws Exception {
        Configuration hadoopConfig = new Configuration(false);
        hadoopConfig.set(PROVIDER_CONFIG_NAME, LEGACY_PROVIDER);

        HadoopS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);

        AWSCredentialProviderList providers =
                CredentialProviderListFactory.buildAWSProviderList(
                        new URI("s3a://test-bucket/"),
                        hadoopConfig,
                        PROVIDER_CONFIG_NAME,
                        Collections.emptyList(),
                        Collections.emptySet());
        assertThat(providers.toString())
                .contains(HadoopDynamicTemporaryAWSCredentialsProvider.NAME)
                .doesNotContain(LEGACY_PROVIDER);
    }
}
