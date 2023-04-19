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

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.apache.flink.fs.s3.common.token.AbstractS3DelegationTokenReceiver.PROVIDER_CONFIG_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for {@link AbstractS3DelegationTokenReceiver}. */
public class AbstractS3DelegationTokenReceiverTest {

    private static final String PROVIDER_CLASS_NAME = "TestProvider";
    private static final String REGION = "testRegion";

    @BeforeEach
    void beforeEach() {
        AbstractS3DelegationTokenReceiver.region = null;
    }

    @AfterEach
    void afterEach() {
        AbstractS3DelegationTokenReceiver.region = null;
    }

    @Test
    public void updateHadoopConfigShouldSetProviderWhenEmpty() {
        org.apache.hadoop.conf.Configuration hadoopConfiguration =
                new org.apache.hadoop.conf.Configuration();
        hadoopConfiguration.set(PROVIDER_CONFIG_NAME, "");
        AbstractS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfiguration);
        assertEquals(
                DynamicTemporaryAWSCredentialsProvider.NAME,
                hadoopConfiguration.get(PROVIDER_CONFIG_NAME));
    }

    @Test
    public void updateHadoopConfigShouldPrependProviderWhenNotEmpty() {
        org.apache.hadoop.conf.Configuration hadoopConfiguration =
                new org.apache.hadoop.conf.Configuration();
        hadoopConfiguration.set(PROVIDER_CONFIG_NAME, PROVIDER_CLASS_NAME);
        AbstractS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfiguration);
        String[] providers = hadoopConfiguration.get(PROVIDER_CONFIG_NAME).split(",");
        assertEquals(2, providers.length);
        assertEquals(DynamicTemporaryAWSCredentialsProvider.NAME, providers[0]);
        assertEquals(PROVIDER_CLASS_NAME, providers[1]);
    }

    @Test
    public void updateHadoopConfigShouldNotAddProviderWhenAlreadyExists() {
        org.apache.hadoop.conf.Configuration hadoopConfiguration =
                new org.apache.hadoop.conf.Configuration();
        hadoopConfiguration.set(PROVIDER_CONFIG_NAME, DynamicTemporaryAWSCredentialsProvider.NAME);
        AbstractS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfiguration);
        assertEquals(
                DynamicTemporaryAWSCredentialsProvider.NAME,
                hadoopConfiguration.get(PROVIDER_CONFIG_NAME));
    }

    @Test
    public void updateHadoopConfigShouldNotUpdateRegionWhenNotConfigured() {
        AbstractS3DelegationTokenReceiver receiver = createReceiver();
        receiver.init(new Configuration());

        org.apache.hadoop.conf.Configuration hadoopConfiguration =
                new org.apache.hadoop.conf.Configuration();
        AbstractS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfiguration);
        assertNull(hadoopConfiguration.get("fs.s3a.endpoint.region"));
    }

    @Test
    public void updateHadoopConfigShouldUpdateRegionWhenConfigured() {
        AbstractS3DelegationTokenReceiver receiver = createReceiver();
        Configuration configuration = new Configuration();
        configuration.setString(CONFIG_PREFIX + ".s3.region", REGION);
        receiver.init(configuration);

        org.apache.hadoop.conf.Configuration hadoopConfiguration =
                new org.apache.hadoop.conf.Configuration();
        AbstractS3DelegationTokenReceiver.updateHadoopConfig(hadoopConfiguration);
        assertEquals(REGION, hadoopConfiguration.get("fs.s3a.endpoint.region"));
    }

    private AbstractS3DelegationTokenReceiver createReceiver() {
        return new AbstractS3DelegationTokenReceiver() {
            @Override
            public String serviceName() {
                return "s3";
            }
        };
    }
}
