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

package org.apache.flink.table.security.token;

import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sun.security.krb5.KrbException;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Optional;

import static java.time.Instant.ofEpochMilli;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_KEYTAB;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link HiveServer2DelegationTokenProvider}. */
public class HiveServer2DelegationTokenProviderITCase {

    @BeforeAll
    public static void setPropertiesToEnableKerberosConfigInit() throws KrbException {
        System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
        System.setProperty("java.security.krb5.kdc", "kdc");
        System.setProperty("java.security.krb5.conf", "/dev/null");
        sun.security.krb5.Config.refresh();
    }

    @AfterAll
    public static void cleanupHadoopConfigs() {
        UserGroupInformation.setConfiguration(new Configuration());
    }

    private class TestHiveServer2DelegationToken extends HiveServer2DelegationTokenIdentifier {
        @Override
        public long getIssueDate() {
            return 1000;
        }
    }

    @Test
    public void delegationTokensRequiredShouldReturnFalseWhenKerberosIsNotEnabled()
            throws Exception {
        HiveServer2DelegationTokenProvider provider = new HiveServer2DelegationTokenProvider();
        provider.init(new org.apache.flink.configuration.Configuration());
        boolean result = provider.delegationTokensRequired();
        assertFalse(result);
    }

    @Test
    public void delegationTokensRequiredShouldReturnFalseWhenHiveMetastoreUrisIsEmpty()
            throws Exception {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(UserGroupInformation.AuthenticationMethod.KERBEROS));
        UserGroupInformation.getCurrentUser()
                .setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        HiveServer2DelegationTokenProvider provider = new HiveServer2DelegationTokenProvider();
        provider.init(new org.apache.flink.configuration.Configuration());
        boolean result = provider.delegationTokensRequired();
        assertFalse(result);
    }

    @Test
    public void delegationTokensRequiredShouldReturnTrueWhenAllConditionsIsRight(
            @TempDir Path tmpDir) throws Exception {
        URL resource =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("test-hive-delegation-token/hive-site.xml");
        HiveConf.setHiveSiteLocation(resource);

        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(UserGroupInformation.AuthenticationMethod.KERBEROS));
        UserGroupInformation.getCurrentUser()
                .setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        HiveServer2DelegationTokenProvider provider = new HiveServer2DelegationTokenProvider();
        org.apache.flink.configuration.Configuration configuration =
                new org.apache.flink.configuration.Configuration();

        final Path keyTab = Files.createFile(tmpDir.resolve("test.keytab"));
        configuration.setString(KERBEROS_LOGIN_KEYTAB, keyTab.toAbsolutePath().toString());
        configuration.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, "test@EXAMPLE.COM");
        provider.init(configuration);
        boolean result = provider.delegationTokensRequired();
        assertTrue(result);
    }

    @Test
    public void getTokenRenewalIntervalShouldReturnRenewalIntervalWhenNoExceptionIsThrown() {
        HiveServer2DelegationTokenProvider provider =
                new HiveServer2DelegationTokenProvider() {
                    @Override
                    long getNewExpiration(Hive hive, String tokenStr) {
                        return 10000;
                    }
                };
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        TestHiveServer2DelegationToken testDelegationToken = new TestHiveServer2DelegationToken();
        Long renewalInterval =
                provider.getTokenRenewalInterval(constantClock, testDelegationToken, null, "test");
        assertEquals(9000L, renewalInterval);
    }

    @Test
    public void getTokenRenewalIntervalShouldThrowExceptionWhenHiveIsNull() {
        HiveServer2DelegationTokenProvider provider = new HiveServer2DelegationTokenProvider();
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        TestHiveServer2DelegationToken testDelegationToken = new TestHiveServer2DelegationToken();
        final String msg = "java.lang.NullPointerException";
        assertThatThrownBy(
                        () ->
                                provider.getTokenRenewalInterval(
                                        constantClock, testDelegationToken, null, "test"))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(msg);
    }

    @Test
    public void getTokenRenewalDateShouldReturnNoneWhenNegativeRenewalInterval() {
        HiveServer2DelegationTokenProvider provider = new HiveServer2DelegationTokenProvider();
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        TestHiveServer2DelegationToken testDelegationToken = new TestHiveServer2DelegationToken();
        assertEquals(
                Optional.empty(),
                provider.getTokenRenewalDate(constantClock, testDelegationToken, -1L));
    }

    @Test
    public void getTokenRenewalDateShouldReturnRenewalDateWhenNotNegativeRenewalInterval() {
        HiveServer2DelegationTokenProvider provider = new HiveServer2DelegationTokenProvider();
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
        TestHiveServer2DelegationToken testDelegationToken = new TestHiveServer2DelegationToken();
        assertEquals(
                Optional.of(10000L),
                provider.getTokenRenewalDate(constantClock, testDelegationToken, 9000L));
    }

    private Configuration getHadoopConfigWithAuthMethod(
            UserGroupInformation.AuthenticationMethod authenticationMethod) {
        Configuration conf = new Configuration(true);
        conf.set("hadoop.security.authentication", authenticationMethod.name());
        return conf;
    }
}
