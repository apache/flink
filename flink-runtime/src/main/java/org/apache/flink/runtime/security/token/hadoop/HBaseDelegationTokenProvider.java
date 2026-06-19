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
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedExceptionAction;
import java.util.Optional;

/**
 * Delegation token provider implementation for HBase. Basically it would be good to move this to
 * flink-connector-hbase-base but HBase connection can be made without the connector. All in all I
 * tend to move this but that would be a breaking change.
 */
@Internal
public class HBaseDelegationTokenProvider implements DelegationTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseDelegationTokenProvider.class);

    org.apache.hadoop.conf.Configuration hbaseConf;

    private KerberosLoginProvider kerberosLoginProvider;

    @Override
    public String serviceName() {
        return "hbase";
    }

    @Override
    public void init(Configuration configuration) throws Exception {
        hbaseConf = getHBaseConfiguration(configuration);
        kerberosLoginProvider = new KerberosLoginProvider(configuration);
    }

    private org.apache.hadoop.conf.Configuration getHBaseConfiguration(Configuration conf) {
        org.apache.hadoop.conf.Configuration hbaseConf = null;
        try {
            org.apache.hadoop.conf.Configuration hadoopConf =
                    HadoopUtils.getHadoopConfiguration(conf);
            // ----
            // Intended call: HBaseConfiguration.create(conf);
            // HBaseConfiguration.create has been added to HBase in v0.90.0 so we can eliminate
            // reflection magic when we drop ancient HBase support.
            hbaseConf =
                    (org.apache.hadoop.conf.Configuration)
                            Class.forName("org.apache.hadoop.hbase.HBaseConfiguration")
                                    .getMethod("create", org.apache.hadoop.conf.Configuration.class)
                                    .invoke(null, hadoopConf);
            // ----
        } catch (InvocationTargetException
                | NoSuchMethodException
                | IllegalAccessException
                | ClassNotFoundException
                | NoClassDefFoundError e) {
            LOG.debug(
                    "HBase is not available (not packaged with this application): {} : \"{}\".",
                    e.getClass().getSimpleName(),
                    e.getMessage());
        }
        return hbaseConf;
    }

    @Override
    public boolean delegationTokensRequired() throws Exception {
        /**
         * The general rule how a provider/receiver must behave is the following: The provider and
         * the receiver must be added to the classpath together with all the additionally required
         * dependencies.
         *
         * <p>This null check is required because the HBase provider is always on classpath but
         * HBase jars are optional. Such case configuration is not able to be loaded. This construct
         * is intended to be removed when HBase provider/receiver pair can be externalized (namely
         * if a provider/receiver throws an exception then workload must be stopped).
         */
        if (hbaseConf == null) {
            LOG.debug(
                    "HBase is not available (not packaged with this application), hence no "
                            + "tokens will be acquired.");
            return false;
        }
        try {
            if (!HadoopUtils.isKerberosSecurityEnabled(UserGroupInformation.getCurrentUser())) {
                return false;
            }
        } catch (IOException e) {
            LOG.debug("Hadoop Kerberos is not enabled.");
            return false;
        }
        return hbaseConf.get("hbase.security.authentication").equals("kerberos")
                && kerberosLoginProvider.isLoginPossible(false);
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        UserGroupInformation freshUGI = kerberosLoginProvider.doLoginAndReturnUGI();
        return freshUGI.doAs(
                (PrivilegedExceptionAction<ObtainedDelegationTokens>)
                        () -> {
                            Token<?> token;
                            Preconditions.checkNotNull(hbaseConf);
                            try {
                                LOG.info("Obtaining Kerberos security token for HBase");
                                // ----
                                // Intended call: Token<AuthenticationTokenIdentifier> token
                                // =
                                // TokenUtil.obtainToken(conf);
                                token =
                                        (Token<?>)
                                                Class.forName(
                                                                "org.apache.hadoop.hbase.security.token.TokenUtil")
                                                        .getMethod(
                                                                "obtainToken",
                                                                org.apache.hadoop.conf.Configuration
                                                                        .class)
                                                        .invoke(null, hbaseConf);
                            } catch (NoSuchMethodException e) {
                                // for HBase 2

                                // ----
                                // Intended call: ConnectionFactory connectionFactory =
                                // ConnectionFactory.createConnection(conf);
                                Closeable connectionFactory =
                                        (Closeable)
                                                Class.forName(
                                                                "org.apache.hadoop.hbase.client.ConnectionFactory")
                                                        .getMethod(
                                                                "createConnection",
                                                                org.apache.hadoop.conf.Configuration
                                                                        .class)
                                                        .invoke(null, hbaseConf);
                                // ----
                                Class<?> connectionClass =
                                        Class.forName("org.apache.hadoop.hbase.client.Connection");
                                // ----
                                // Intended call: Token<AuthenticationTokenIdentifier> token
                                // =
                                // TokenUtil.obtainToken(connectionFactory);
                                token =
                                        (Token<?>)
                                                Class.forName(
                                                                "org.apache.hadoop.hbase.security.token.TokenUtil")
                                                        .getMethod("obtainToken", connectionClass)
                                                        .invoke(null, connectionFactory);
                                if (null != connectionFactory) {
                                    connectionFactory.close();
                                }
                            }

                            Credentials credentials = new Credentials();
                            credentials.addToken(token.getService(), token);

                            // HBase does not support to renew the delegation token currently
                            // https://cwiki.apache.org/confluence/display/HADOOP2/Hbase+HBaseTokenAuthentication
                            return new ObtainedDelegationTokens(
                                    HadoopDelegationTokenConverter.serialize(credentials),
                                    Optional.empty());
                        });
    }
}
