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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link AbstractS3DelegationTokenProvider}. */
public class AbstractS3DelegationTokenProviderTest {

    private static final String REGION = "testRegion";
    private static final String ACCESS_KEY_ID = "testAccessKeyId";
    private static final String SECRET_ACCESS_KEY = "testSecretAccessKey";

    private AbstractS3DelegationTokenProvider provider;

    @BeforeEach
    public void beforeEach() {
        provider =
                new AbstractS3DelegationTokenProvider() {
                    @Override
                    public String serviceName() {
                        return "s3";
                    }
                };
    }

    @Test
    public void delegationTokensRequiredShouldReturnFalseWithoutCredentials() {
        provider.init(new Configuration());
        assertFalse(provider.delegationTokensRequired());
    }

    @Test
    public void delegationTokensRequiredShouldReturnTrueWithCredentials() {
        Configuration configuration = new Configuration();
        configuration.setString(CONFIG_PREFIX + ".s3.region", REGION);
        configuration.setString(CONFIG_PREFIX + ".s3.access-key", ACCESS_KEY_ID);
        configuration.setString(CONFIG_PREFIX + ".s3.secret-key", SECRET_ACCESS_KEY);
        provider.init(configuration);

        assertTrue(provider.delegationTokensRequired());
    }
}
