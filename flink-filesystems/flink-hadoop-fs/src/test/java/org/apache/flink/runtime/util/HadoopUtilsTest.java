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

package org.apache.flink.runtime.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import sun.security.krb5.KrbException;

import static org.apache.flink.runtime.util.HadoopUtils.HDFS_DELEGATION_TOKEN_KIND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Unit tests for Hadoop utils. */
class HadoopUtilsTest {

    @BeforeAll
    static void setPropertiesToEnableKerberosConfigInit() throws KrbException {
        System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
        System.setProperty("java.security.krb5.kdc", "kdc");
        System.setProperty("java.security.krb5.conf", "/dev/null");
        sun.security.krb5.Config.refresh();
    }

    @AfterAll
    static void cleanupHadoopConfigs() {
        UserGroupInformation.setConfiguration(new Configuration());
    }

    @Test
    void testShouldReturnFalseWhenNoKerberosCredentialsOrDelegationTokens() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(AuthenticationMethod.KERBEROS));
        UserGroupInformation userWithoutCredentialsOrTokens =
                createTestUser(AuthenticationMethod.KERBEROS);
        assumeThat(userWithoutCredentialsOrTokens.hasKerberosCredentials()).isFalse();

        boolean isKerberosEnabled =
                HadoopUtils.isKerberosSecurityEnabled(userWithoutCredentialsOrTokens);
        boolean result =
                HadoopUtils.areKerberosCredentialsValid(userWithoutCredentialsOrTokens, true);

        assertThat(isKerberosEnabled).isTrue();
        assertThat(result).isFalse();
    }

    @Test
    void testShouldReturnTrueWhenDelegationTokenIsPresent() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(AuthenticationMethod.KERBEROS));
        UserGroupInformation userWithoutCredentialsButHavingToken =
                createTestUser(AuthenticationMethod.KERBEROS);
        userWithoutCredentialsButHavingToken.addToken(getHDFSDelegationToken());
        assumeThat(userWithoutCredentialsButHavingToken.hasKerberosCredentials()).isFalse();

        boolean result =
                HadoopUtils.areKerberosCredentialsValid(userWithoutCredentialsButHavingToken, true);

        assertThat(result).isTrue();
    }

    @Test
    void testShouldReturnTrueWhenKerberosCredentialsArePresent() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(AuthenticationMethod.KERBEROS));
        UserGroupInformation userWithCredentials = Mockito.mock(UserGroupInformation.class);
        Mockito.when(userWithCredentials.getAuthenticationMethod())
                .thenReturn(AuthenticationMethod.KERBEROS);
        Mockito.when(userWithCredentials.hasKerberosCredentials()).thenReturn(true);

        boolean result = HadoopUtils.areKerberosCredentialsValid(userWithCredentials, true);

        assertThat(result).isTrue();
    }

    @Test
    void isKerberosSecurityEnabled_NoKerberos_ReturnsFalse() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(AuthenticationMethod.PROXY));
        UserGroupInformation userWithAuthMethodOtherThanKerberos =
                createTestUser(AuthenticationMethod.PROXY);

        boolean result = HadoopUtils.isKerberosSecurityEnabled(userWithAuthMethodOtherThanKerberos);

        assertThat(result).isFalse();
    }

    @Test
    void testShouldReturnTrueIfTicketCacheIsNotUsed() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(AuthenticationMethod.KERBEROS));
        UserGroupInformation user = createTestUser(AuthenticationMethod.KERBEROS);

        boolean result = HadoopUtils.areKerberosCredentialsValid(user, false);

        assertThat(result).isTrue();
    }

    @Test
    void testShouldCheckIfTheUserHasHDFSDelegationToken() {
        UserGroupInformation userWithToken = createTestUser(AuthenticationMethod.KERBEROS);
        userWithToken.addToken(getHDFSDelegationToken());

        boolean result = HadoopUtils.hasHDFSDelegationToken(userWithToken);

        assertThat(result).isTrue();
    }

    @Test
    void testShouldReturnFalseIfTheUserHasNoHDFSDelegationToken() {
        UserGroupInformation userWithoutToken = createTestUser(AuthenticationMethod.KERBEROS);
        assumeThat(userWithoutToken.getTokens().isEmpty()).isTrue();

        boolean result = HadoopUtils.hasHDFSDelegationToken(userWithoutToken);

        assertThat(result).isFalse();
    }

    private static Configuration getHadoopConfigWithAuthMethod(
            AuthenticationMethod authenticationMethod) {
        Configuration conf = new Configuration(true);
        conf.set("hadoop.security.authentication", authenticationMethod.name());
        return conf;
    }

    private static UserGroupInformation createTestUser(AuthenticationMethod authenticationMethod) {
        UserGroupInformation user = UserGroupInformation.createRemoteUser("test-user");
        user.setAuthenticationMethod(authenticationMethod);
        return user;
    }

    private static Token<DelegationTokenIdentifier> getHDFSDelegationToken() {
        Token<DelegationTokenIdentifier> token = new Token<>();
        token.setKind(HDFS_DELEGATION_TOKEN_KIND);
        return token;
    }
}
