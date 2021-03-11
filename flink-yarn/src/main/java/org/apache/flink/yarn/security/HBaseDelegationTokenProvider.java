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

package org.apache.flink.yarn.security;

import org.apache.flink.runtime.security.delegationtokens.HadoopDelegationTokenConfiguration;
import org.apache.flink.runtime.security.delegationtokens.HadoopDelegationTokenProvider;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

/** Delegation token provider implementation for HBase. */
public class HBaseDelegationTokenProvider implements HadoopDelegationTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseDelegationTokenProvider.class);

    private Configuration hadoopConf;

    private Configuration hbaseConf;

    @Override
    public String serviceName() {
        return "hbase";
    }

    @Override
    public void init(HadoopDelegationTokenConfiguration conf) {
        this.hadoopConf = conf.getHadoopConf();
        this.hbaseConf = null;
    }

    @Override
    public boolean delegationTokensRequired() {
        if (UserGroupInformation.isSecurityEnabled()) {
            hbaseConf = createHBaseConfiguration(hadoopConf);
            if (hbaseConf != null) {
                LOG.info(
                        "HBase security setting: {}",
                        hbaseConf.get("hbase.security.authentication"));

                boolean required =
                        "kerberos".equals(hbaseConf.get("hbase.security.authentication"));
                if (!required) {
                    LOG.info("HBase has not been configured to use Kerberos.");
                }
                return required;
            }
        }
        return false;
    }

    private Configuration createHBaseConfiguration(Configuration conf) {
        try {
            // ----
            // Intended call: HBaseConfiguration.create(conf);
            return (Configuration)
                    Class.forName("org.apache.hadoop.hbase.HBaseConfiguration")
                            .getMethod("create", org.apache.hadoop.conf.Configuration.class)
                            .invoke(null, conf);
            // ----

        } catch (InvocationTargetException
                | NoSuchMethodException
                | IllegalAccessException
                | ClassNotFoundException e) {
            LOG.info(
                    "HBase is not available (not packaged with this application): {} : \"{}\".",
                    e.getClass().getSimpleName(),
                    e.getMessage());
        }
        return null;
    }

    @Override
    public Optional<Long> obtainDelegationTokens(final Credentials credentials) {
        Token<?> token;
        try {
            Preconditions.checkNotNull(hbaseConf);
            try {
                LOG.info("Obtaining Kerberos security token for HBase");
                // ----
                // Intended call: Token<AuthenticationTokenIdentifier> token =
                // TokenUtil.obtainToken(conf);
                token =
                        (Token<?>)
                                Class.forName("org.apache.hadoop.hbase.security.token.TokenUtil")
                                        .getMethod(
                                                "obtainToken",
                                                org.apache.hadoop.conf.Configuration.class)
                                        .invoke(null, hbaseConf);
            } catch (NoSuchMethodException e) {
                // for HBase 2

                // ----
                // Intended call: ConnectionFactory connectionFactory =
                // ConnectionFactory.createConnection(conf);
                Closeable connectionFactory =
                        (Closeable)
                                Class.forName("org.apache.hadoop.hbase.client.ConnectionFactory")
                                        .getMethod(
                                                "createConnection",
                                                org.apache.hadoop.conf.Configuration.class)
                                        .invoke(null, hbaseConf);
                // ----
                Class<?> connectionClass =
                        Class.forName("org.apache.hadoop.hbase.client.Connection");
                // ----
                // Intended call: Token<AuthenticationTokenIdentifier> token =
                // TokenUtil.obtainToken(connectionFactory);
                token =
                        (Token<?>)
                                Class.forName("org.apache.hadoop.hbase.security.token.TokenUtil")
                                        .getMethod("obtainToken", connectionClass)
                                        .invoke(null, connectionFactory);
                if (null != connectionFactory) {
                    connectionFactory.close();
                }
            }
            if (token == null) {
                LOG.error("No Kerberos security token for HBase available");
            }

            credentials.addToken(token.getService(), token);
            LOG.info("Added HBase Kerberos security token to credentials.");
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | IllegalAccessException
                | InvocationTargetException
                | IOException e) {
            LOG.info(
                    "HBase is not available (failed to obtain delegation tokens): {} : \"{}\".",
                    e.getClass().getSimpleName(),
                    e.getMessage());
        }

        // Flink does not support to renew the delegation token currently
        return Optional.empty();
    }
}
