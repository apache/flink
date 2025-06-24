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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenReceiver;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hadoop delegation token receiver base class. */
@Internal
public abstract class HadoopDelegationTokenReceiver implements DelegationTokenReceiver {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public abstract String serviceName();

    public void init(Configuration configuration) throws Exception {}

    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        if (tokens == null || tokens.length == 0) {
            throw new IllegalArgumentException("Illegal tokens tried to be processed");
        }
        Credentials credentials = HadoopDelegationTokenConverter.deserialize(tokens);

        log.info("Updating delegation tokens for current user");
        dumpAllTokens(credentials);
        UserGroupInformation.getCurrentUser().addCredentials(credentials);
        log.info("Updated delegation tokens for current user successfully");
    }

    private void dumpAllTokens(Credentials credentials) {
        credentials
                .getAllTokens()
                .forEach(
                        token ->
                                log.info(
                                        "Token Service:{} Identifier:{}",
                                        token.getService(),
                                        token.getIdentifier()));
    }
}
