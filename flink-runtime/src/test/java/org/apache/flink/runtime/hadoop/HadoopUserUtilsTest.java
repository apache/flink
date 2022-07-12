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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sun.security.krb5.KrbException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for Hadoop user utils. */
class HadoopUserUtilsTest {

    @BeforeAll
    public static void setPropertiesToEnableKerberosConfigInit() throws KrbException {
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
        System.setProperty("java.security.krb5.conf", "/dev/null");
        sun.security.krb5.Config.refresh();
    }

    @AfterAll
    public static void cleanupHadoopConfigs() {
        UserGroupInformation.setConfiguration(new Configuration());
    }

    @Test
    public void testIsProxyUserShouldReturnFalseWhenNormalUser() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(AuthenticationMethod.KERBEROS));
        UserGroupInformation userGroupInformation = createTestUser(AuthenticationMethod.KERBEROS);

        assertFalse(HadoopUserUtils.isProxyUser(userGroupInformation));
    }

    @Test
    public void testIsProxyUserShouldReturnTrueWhenProxyUser() {
        UserGroupInformation.setConfiguration(
                getHadoopConfigWithAuthMethod(AuthenticationMethod.KERBEROS));
        UserGroupInformation userGroupInformation = createTestUser(AuthenticationMethod.PROXY);

        assertTrue(HadoopUserUtils.isProxyUser(userGroupInformation));
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
}
