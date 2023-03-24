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

package org.apache.flink.runtime.security.token.hadoop;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Test for {@link HadoopDelegationTokenReceiver}. */
public class HadoopDelegationTokenReceiverITCase {

    @Test
    public void onNewTokensObtainedShouldThrowExceptionWhenNullCredentials() {
        onNewTokensObtainedShouldThrowException(null);
    }

    @Test
    public void onNewTokensObtainedShouldThrowExceptionWhenEmptyCredentials() {
        onNewTokensObtainedShouldThrowException(new byte[0]);
    }

    private void onNewTokensObtainedShouldThrowException(byte[] credentialsBytes) {
        HadoopDelegationTokenReceiver receiver =
                new HadoopDelegationTokenReceiver() {
                    @Override
                    public String serviceName() {
                        return "test";
                    }
                };
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> receiver.onNewTokensObtained(credentialsBytes));
        assertTrue(e.getMessage().contains("Illegal tokens"));
    }

    @Test
    public void onNewTokensObtainedShouldOverwriteCredentials() throws Exception {
        final Text tokenKind = new Text("TEST_TOKEN_KIND");
        final Text tokenService = new Text("TEST_TOKEN_SERVICE");
        Credentials credentials = new Credentials();
        credentials.addToken(
                tokenService, new Token<>(new byte[4], new byte[4], tokenKind, tokenService));
        byte[] credentialsBytes = HadoopDelegationTokenConverter.serialize(credentials);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            HadoopDelegationTokenReceiver receiver =
                    new HadoopDelegationTokenReceiver() {
                        @Override
                        public String serviceName() {
                            return "test";
                        }
                    };
            receiver.onNewTokensObtained(credentialsBytes);
            ArgumentCaptor<Credentials> argumentCaptor = ArgumentCaptor.forClass(Credentials.class);
            verify(userGroupInformation, times(1)).addCredentials(argumentCaptor.capture());
            assertTrue(
                    CollectionUtils.isEqualCollection(
                            credentials.getAllTokens(), argumentCaptor.getValue().getAllTokens()));
        }
    }
}
