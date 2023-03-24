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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Test for {@link HadoopDelegationTokenConverter}. */
public class HadoopDelegationTokenConverterTest {

    @Test
    public void testRoundTrip() throws IOException, ClassNotFoundException {
        final Text tokenKind = new Text("TEST_TOKEN_KIND");
        final Text tokenService = new Text("TEST_TOKEN_SERVICE");
        Credentials credentials = new Credentials();
        credentials.addToken(
                tokenService, new Token<>(new byte[4], new byte[4], tokenKind, tokenService));

        byte[] credentialsBytes = HadoopDelegationTokenConverter.serialize(credentials);
        Credentials deserializedCredentials =
                HadoopDelegationTokenConverter.deserialize(credentialsBytes);

        assertEquals(1, credentials.getAllTokens().size());
        assertEquals(1, deserializedCredentials.getAllTokens().size());
        assertNotNull(credentials.getToken(tokenService));
        assertNotNull(deserializedCredentials.getToken(tokenService));
    }
}
