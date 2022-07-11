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

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_KEYTAB;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_PRINCIPAL;
import static org.apache.flink.configuration.SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Test for {@link KerberosLoginProvider}.
 *
 * <p>This class is an ITCase because the mocking breaks the {@link UserGroupInformation} class for
 * other tests.
 */
public class KerberosLoginProviderITCase {

    @Test
    public void isLoginPossibleMustReturnFalseByDefault() throws IOException {
        Configuration configuration = new Configuration();
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertFalse(kerberosLoginProvider.isLoginPossible());
        }
    }

    @Test
    public void isLoginPossibleMustReturnTrueWithKeytab(@TempDir Path tmpDir) throws IOException {
        Configuration configuration = new Configuration();
        configuration.setString(KERBEROS_LOGIN_PRINCIPAL, "principal");
        final Path keyTab = Files.createFile(tmpDir.resolve("test.keytab"));
        configuration.setString(KERBEROS_LOGIN_KEYTAB, keyTab.toAbsolutePath().toString());
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertTrue(kerberosLoginProvider.isLoginPossible());
        }
    }

    @Test
    public void isLoginPossibleMustReturnTrueWithTGT() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setBoolean(KERBEROS_LOGIN_USETICKETCACHE, true);
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.getAuthenticationMethod())
                    .thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
            when(userGroupInformation.hasKerberosCredentials()).thenReturn(true);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertTrue(kerberosLoginProvider.isLoginPossible());
        }
    }

    @Test
    public void isLoginPossibleMustThrowExceptionWithProxyUser() {
        Configuration configuration = new Configuration();
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.getAuthenticationMethod())
                    .thenReturn(UserGroupInformation.AuthenticationMethod.PROXY);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertThrows(
                    UnsupportedOperationException.class, kerberosLoginProvider::isLoginPossible);
        }
    }

    @Test
    public void doLoginMustLoginWithKeytab(@TempDir Path tmpDir) throws IOException {
        Configuration configuration = new Configuration();
        configuration.setString(KERBEROS_LOGIN_PRINCIPAL, "principal");
        final Path keyTab = Files.createFile(tmpDir.resolve("test.keytab"));
        configuration.setString(KERBEROS_LOGIN_KEYTAB, keyTab.toAbsolutePath().toString());
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            kerberosLoginProvider.doLogin();
            ugi.verify(
                    () ->
                            UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                                    anyString(), anyString()));
        }
    }

    @Test
    public void doLoginMustLoginWithTGT() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setBoolean(KERBEROS_LOGIN_USETICKETCACHE, true);
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.getAuthenticationMethod())
                    .thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
            when(userGroupInformation.hasKerberosCredentials()).thenReturn(true);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            kerberosLoginProvider.doLogin();
            ugi.verify(() -> UserGroupInformation.getUGIFromTicketCache(null, null));
        }
    }

    @Test
    public void doLoginMustThrowExceptionWithProxyUser() {
        Configuration configuration = new Configuration();
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.getAuthenticationMethod())
                    .thenReturn(UserGroupInformation.AuthenticationMethod.PROXY);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertThrows(UnsupportedOperationException.class, kerberosLoginProvider::doLogin);
        }
    }
}
