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

package org.apache.flink.yarn.token;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.runtime.security.token.hadoop.HadoopDelegationTokenConverter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;
import java.util.Optional;

public class TestYarnAMDelegationTokenProvider implements DelegationTokenProvider {

    public static final String SERVICE_NAME = "yarn-am";
    public static final Token<? extends TokenIdentifier> TEST_YARN_AM_TOKEN =
            new Token<>(
                    new byte[4],
                    new byte[4],
                    new Text("TEST_YARN_AM_TOKEN_KIND"),
                    new Text("TEST_YARN_AM_TOKEN_SERVICE"));

    @Override
    public String serviceName() {
        return SERVICE_NAME;
    }

    @Override
    public void init(Configuration configuration) {}

    @Override
    public boolean delegationTokensRequired() {
        return true;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws IOException {
        Credentials credentials = new Credentials();
        credentials.addToken(TEST_YARN_AM_TOKEN.getService(), TEST_YARN_AM_TOKEN);
        return new ObtainedDelegationTokens(
                HadoopDelegationTokenConverter.serialize(credentials), Optional.empty());
    }
}
