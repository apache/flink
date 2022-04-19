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

package org.apache.flink.runtime.security.token;

import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link DelegationTokenManager}. */
public class KerberosDelegationTokenManagerTest {

    @Test
    public void isProviderEnabledMustGiveBackTrueByDefault() {
        ExceptionThrowingDelegationTokenProvider.enabled = false;
        Configuration configuration = new Configuration();
        KerberosDelegationTokenManager delegationTokenManager =
                new KerberosDelegationTokenManager(configuration);

        assertTrue(delegationTokenManager.isProviderEnabled("test"));
    }

    @Test
    public void isProviderEnabledMustGiveBackFalseWhenDisabled() {
        ExceptionThrowingDelegationTokenProvider.enabled = false;
        Configuration configuration = new Configuration();
        configuration.setBoolean("security.kerberos.token.provider.test.enabled", false);
        KerberosDelegationTokenManager delegationTokenManager =
                new KerberosDelegationTokenManager(configuration);

        assertFalse(delegationTokenManager.isProviderEnabled("test"));
    }

    @Test(expected = Exception.class)
    public void configurationIsNullMustFailFast() {
        new KerberosDelegationTokenManager(null);
    }

    @Test(expected = Exception.class)
    public void oneProviderThrowsExceptionMustFailFast() {
        try {
            ExceptionThrowingDelegationTokenProvider.enabled = true;
            new KerberosDelegationTokenManager(new Configuration());
        } finally {
            ExceptionThrowingDelegationTokenProvider.enabled = false;
        }
    }

    @Test
    public void testAllProvidersLoaded() {
        ExceptionThrowingDelegationTokenProvider.enabled = false;
        ExceptionThrowingDelegationTokenProvider.constructed = false;
        Configuration configuration = new Configuration();
        configuration.setBoolean("security.kerberos.token.provider.throw.enabled", false);
        KerberosDelegationTokenManager delegationTokenManager =
                new KerberosDelegationTokenManager(configuration);

        assertEquals(1, delegationTokenManager.delegationTokenProviders.size());
        assertTrue(delegationTokenManager.isProviderLoaded("test"));
        assertTrue(ExceptionThrowingDelegationTokenProvider.constructed);
        assertFalse(delegationTokenManager.isProviderLoaded("throw"));
    }
}
