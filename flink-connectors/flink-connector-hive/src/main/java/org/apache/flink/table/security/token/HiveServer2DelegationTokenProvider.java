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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.runtime.security.token.hadoop.HadoopDelegationTokenConverter;
import org.apache.flink.runtime.security.token.hadoop.KerberosLoginProvider;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.Clock;
import java.util.Optional;

import static org.apache.flink.runtime.hadoop.HadoopUserUtils.getIssueDate;

/** Delegation token provider for HiveServer2. */
@Internal
public class HiveServer2DelegationTokenProvider implements DelegationTokenProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveServer2DelegationTokenProvider.class);

    org.apache.hadoop.conf.Configuration hiveConf;

    private KerberosLoginProvider kerberosLoginProvider;

    private Long tokenRenewalInterval;

    @Override
    public String serviceName() {
        return "HiveServer2";
    }

    @Override
    public void init(Configuration configuration) throws Exception {
        hiveConf = getHiveConfiguration(configuration);
        kerberosLoginProvider = new KerberosLoginProvider(configuration);
    }

    private org.apache.hadoop.conf.Configuration getHiveConfiguration(Configuration conf) {
        try {
            org.apache.hadoop.conf.Configuration hadoopConf =
                    HadoopUtils.getHadoopConfiguration(conf);
            hiveConf = new HiveConf(hadoopConf, HiveConf.class);
        } catch (Exception | NoClassDefFoundError e) {
            LOG.warn("Fail to create HiveServer2 Configuration", e);
        }
        return hiveConf;
    }

    @Override
    public boolean delegationTokensRequired() throws Exception {
        /**
         * The general rule how a provider/receiver must behave is the following: The provider and
         * the receiver must be added to the classpath together with all the additionally required
         * dependencies.
         *
         * <p>This null check is required because the HiveServer2 provider is always on classpath
         * but Hive jars are optional. Such case configuration is not able to be loaded. This
         * construct is intended to be removed when HiveServer2 provider/receiver pair can be
         * externalized (namely if a provider/receiver throws an exception then workload must be
         * stopped).
         */
        if (hiveConf == null) {
            LOG.debug(
                    "HiveServer2 is not available (not packaged with this application), hence no "
                            + "hiveServer2 tokens will be acquired.");
            return false;
        }
        try {
            if (!HadoopUtils.isKerberosSecurityEnabled(UserGroupInformation.getCurrentUser())) {
                LOG.debug(
                        "Hadoop Kerberos is not enabled,hence no hiveServer2 tokens will be acquired.");
                return false;
            }
        } catch (IOException e) {
            LOG.debug(
                    "Hadoop Kerberos is not enabled,hence no hiveServer2 tokens will be acquired.",
                    e);
            return false;
        }

        if (hiveConf.getTrimmed("hive.metastore.uris", "").isEmpty()) {
            LOG.debug(
                    "The hive.metastore.uris item is empty,hence no hiveServer2 tokens will be acquired.");
            return false;
        }

        if (!kerberosLoginProvider.isLoginPossible(false)) {
            LOG.debug("Login is NOT possible,hence no hiveServer2 tokens will be acquired.");
            return false;
        }

        return true;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        UserGroupInformation freshUGI = kerberosLoginProvider.doLoginAndReturnUGI();
        return freshUGI.doAs(
                (PrivilegedExceptionAction<ObtainedDelegationTokens>)
                        () -> {
                            Preconditions.checkNotNull(hiveConf);
                            Hive hive = Hive.get((HiveConf) hiveConf);
                            Clock clock = Clock.systemDefaultZone();
                            try {
                                LOG.info("Obtaining Kerberos security token for HiveServer2");

                                String principal =
                                        hiveConf.getTrimmed(
                                                "hive.metastore.kerberos.principal", "");

                                String tokenStr =
                                        hive.getDelegationToken(
                                                UserGroupInformation.getCurrentUser().getUserName(),
                                                principal);
                                Token<HiveServer2DelegationTokenIdentifier> hive2Token =
                                        new Token<>();
                                hive2Token.decodeFromUrlString(tokenStr);

                                Credentials credentials = new Credentials();
                                credentials.addToken(hive2Token.getKind(), hive2Token);

                                HiveServer2DelegationTokenIdentifier tokenIdentifier =
                                        hive2Token.decodeIdentifier();

                                if (tokenRenewalInterval == null) {
                                    tokenRenewalInterval =
                                            getTokenRenewalInterval(
                                                    clock, tokenIdentifier, hive, tokenStr);
                                }
                                Optional<Long> validUntil =
                                        getTokenRenewalDate(
                                                clock, tokenIdentifier, tokenRenewalInterval);

                                return new ObtainedDelegationTokens(
                                        HadoopDelegationTokenConverter.serialize(credentials),
                                        validUntil);

                            } catch (Exception e) {
                                LOG.error(
                                        "Failed to obtain delegation token for {}",
                                        this.serviceName(),
                                        e);
                                throw new FlinkRuntimeException(e);
                            } finally {
                                Hive.closeCurrent();
                            }
                        });
    }

    @VisibleForTesting
    Long getTokenRenewalInterval(
            Clock clock,
            HiveServer2DelegationTokenIdentifier tokenIdentifier,
            Hive hive,
            String tokenStr) {
        Long interval;
        LOG.debug("Got Delegation token is {} ", tokenIdentifier);
        long newExpiration = getNewExpiration(hive, tokenStr);
        String tokenKind = tokenIdentifier.getKind().toString();
        interval = newExpiration - getIssueDate(clock, tokenKind, tokenIdentifier);
        LOG.info("Renewal interval is {} for token {}", interval, tokenKind);
        return interval;
    }

    @VisibleForTesting
    long getNewExpiration(Hive hive, String tokenStr) {
        try {
            return hive.getMSC().renewDelegationToken(tokenStr);
        } catch (Exception e) {
            LOG.error("renew Delegation Token failed", e);
            throw new FlinkRuntimeException(e);
        }
    }

    @VisibleForTesting
    Optional<Long> getTokenRenewalDate(
            Clock clock,
            HiveServer2DelegationTokenIdentifier tokenIdentifier,
            Long renewalInterval) {
        if (renewalInterval < 0) {
            LOG.debug("Negative renewal interval so no renewal date is calculated");
            return Optional.empty();
        }

        try {
            String tokenKind = tokenIdentifier.getKind().toString();
            long date = getIssueDate(clock, tokenKind, tokenIdentifier) + renewalInterval;
            LOG.debug("Renewal date is {} for token {}", date, tokenKind);
            return Optional.of(date);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
