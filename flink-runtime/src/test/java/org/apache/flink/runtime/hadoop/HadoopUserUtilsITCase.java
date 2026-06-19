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

package org.apache.flink.runtime.hadoop;

import org.apache.flink.runtime.security.token.hadoop.TestHadoopDelegationTokenIdentifier;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.Clock;
import java.time.ZoneId;

import static java.time.Instant.ofEpochMilli;
import static org.apache.flink.runtime.hadoop.HadoopUserUtils.getIssueDate;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.PROXY;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.SIMPLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for Hadoop user utils.
 *
 * <p>The singleton design of {@link UserGroupInformation} prevents using the best practice of
 * implementing a reusable test utility around it, consequently had to resort to relying on mockito.
 */
class HadoopUserUtilsITCase {
    private static final long NOW = 100;

    @Test
    public void isProxyUserShouldReturnFalseWhenNormalUser() {
        UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
        when(userGroupInformation.getAuthenticationMethod()).thenReturn(SIMPLE);

        assertFalse(HadoopUserUtils.isProxyUser(userGroupInformation));
    }

    @Test
    public void isProxyUserShouldReturnTrueWhenProxyUser() {
        UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
        when(userGroupInformation.getAuthenticationMethod()).thenReturn(PROXY);

        assertTrue(HadoopUserUtils.isProxyUser(userGroupInformation));
    }

    @Test
    public void hasUserKerberosAuthMethodShouldReturnFalseWhenNoSecurity() {
        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::isSecurityEnabled).thenReturn(false);

            assertFalse(HadoopUserUtils.hasUserKerberosAuthMethod(userGroupInformation));
        }
    }

    @Test
    public void hasUserKerberosAuthMethodShouldReturnFalseWithSecurityAndNoKerberos() {
        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.getAuthenticationMethod()).thenReturn(SIMPLE);
            ugi.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);

            assertFalse(HadoopUserUtils.hasUserKerberosAuthMethod(userGroupInformation));
        }
    }

    @Test
    public void hasUserKerberosAuthMethodShouldReturnTrueWithSecurityAndKerberos() {
        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.getAuthenticationMethod()).thenReturn(KERBEROS);
            ugi.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);

            assertTrue(HadoopUserUtils.hasUserKerberosAuthMethod(userGroupInformation));
        }
    }

    @Test
    public void getIssueDateShouldReturnIssueDateWithFutureToken() {
        Clock constantClock = Clock.fixed(ofEpochMilli(NOW), ZoneId.systemDefault());
        long issueDate = NOW + 1;
        AbstractDelegationTokenIdentifier tokenIdentifier =
                new TestHadoopDelegationTokenIdentifier(issueDate);

        assertEquals(
                issueDate,
                getIssueDate(constantClock, tokenIdentifier.getKind().toString(), tokenIdentifier));
    }

    @Test
    public void getIssueDateShouldReturnIssueDateWithPastToken() {
        Clock constantClock = Clock.fixed(ofEpochMilli(NOW), ZoneId.systemDefault());
        long issueDate = NOW - 1;
        AbstractDelegationTokenIdentifier tokenIdentifier =
                new TestHadoopDelegationTokenIdentifier(issueDate);

        assertEquals(
                issueDate,
                getIssueDate(constantClock, tokenIdentifier.getKind().toString(), tokenIdentifier));
    }

    @Test
    public void getIssueDateShouldReturnNowWithInvalidToken() {
        Clock constantClock = Clock.fixed(ofEpochMilli(NOW), ZoneId.systemDefault());
        long issueDate = -1;
        AbstractDelegationTokenIdentifier tokenIdentifier =
                new TestHadoopDelegationTokenIdentifier(issueDate);

        assertEquals(
                NOW,
                getIssueDate(constantClock, tokenIdentifier.getKind().toString(), tokenIdentifier));
    }
}
