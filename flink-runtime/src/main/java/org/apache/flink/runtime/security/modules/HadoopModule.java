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

package org.apache.flink.runtime.security.modules;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.hadoop.HadoopUserUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.token.hadoop.KerberosLoginProvider;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Responsible for installing a Hadoop login user. */
public class HadoopModule implements SecurityModule {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopModule.class);

    private final SecurityConfiguration securityConfig;

    private final Configuration hadoopConfiguration;

    @Nullable private ScheduledExecutorService tgtRenewalExecutorService;

    public HadoopModule(
            SecurityConfiguration securityConfiguration, Configuration hadoopConfiguration) {
        this.securityConfig = checkNotNull(securityConfiguration);
        this.hadoopConfiguration = checkNotNull(hadoopConfiguration);
    }

    @VisibleForTesting
    public SecurityConfiguration getSecurityConfig() {
        return securityConfig;
    }

    @Override
    public void install() throws SecurityInstallException {

        UserGroupInformation.setConfiguration(hadoopConfiguration);

        UserGroupInformation loginUser;

        try {
            KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(securityConfig);
            if (kerberosLoginProvider.isLoginPossible(true)) {
                kerberosLoginProvider.doLogin(true);
                loginUser = UserGroupInformation.getLoginUser();

                if (HadoopUserUtils.isProxyUser((loginUser))
                        && securityConfig
                                .getFlinkConfig()
                                .getBoolean(SecurityOptions.DELEGATION_TOKENS_ENABLED)) {
                    throw new UnsupportedOperationException(
                            "Hadoop Proxy user is supported only when"
                                    + " delegation tokens fetch is managed outside of Flink!"
                                    + " Please try again with "
                                    + SecurityOptions.DELEGATION_TOKENS_ENABLED.key()
                                    + " config set to false!");
                }

                if (loginUser.isFromKeytab()) {
                    String fileLocation =
                            System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
                    if (fileLocation != null) {
                        Credentials credentials =
                                Credentials.readTokenStorageFile(
                                        new File(fileLocation), hadoopConfiguration);
                        loginUser.addCredentials(credentials);
                    }
                    tgtRenewalExecutorService =
                            Executors.newSingleThreadScheduledExecutor(
                                    new ExecutorThreadFactory("TGTRenewalExecutorService"));
                    startTGTRenewal(tgtRenewalExecutorService, loginUser);
                }
            } else {
                loginUser = UserGroupInformation.getLoginUser();
            }

            LOG.info("Hadoop user set to {}", loginUser);
            boolean isKerberosSecurityEnabled =
                    HadoopUserUtils.hasUserKerberosAuthMethod(loginUser);
            LOG.info(
                    "Kerberos security is {}.", isKerberosSecurityEnabled ? "enabled" : "disabled");
            if (isKerberosSecurityEnabled) {
                LOG.info(
                        "Kerberos credentials are {}.",
                        loginUser.hasKerberosCredentials() ? "valid" : "invalid");
            }
        } catch (Throwable ex) {
            throw new SecurityInstallException("Unable to set the Hadoop login user", ex);
        }
    }

    @VisibleForTesting
    void startTGTRenewal(
            ScheduledExecutorService tgtRenewalExecutorService, UserGroupInformation loginUser) {
        LOG.info("Starting TGT renewal task");

        long tgtRenewalPeriod = securityConfig.getTgtRenewalPeriod().toMillis();
        tgtRenewalExecutorService.scheduleAtFixedRate(
                () -> {
                    // In Hadoop 2.x, renewal of the keytab-based login seems to be automatic, but
                    // in Hadoop
                    // 3.x, it is configurable (see
                    // hadoop.kerberos.keytab.login.autorenewal.enabled, added
                    // in HADOOP-9567). This task will make sure that the user stays logged in
                    // regardless of
                    // that configuration's value. Note that checkTGTAndReloginFromKeytab() is a
                    // no-op if
                    // the TGT does not need to be renewed yet.
                    try {
                        LOG.debug("Renewing TGT");
                        loginUser.checkTGTAndReloginFromKeytab();
                        LOG.debug("TGT renewed successfully");
                    } catch (Exception e) {
                        LOG.warn("Error while renewing TGT", e);
                    }
                },
                tgtRenewalPeriod,
                tgtRenewalPeriod,
                TimeUnit.MILLISECONDS);

        LOG.info("TGT renewal task started and reoccur in {} ms", tgtRenewalPeriod);
    }

    @VisibleForTesting
    void stopTGTRenewal() {
        if (tgtRenewalExecutorService != null) {
            tgtRenewalExecutorService.shutdown();
            tgtRenewalExecutorService = null;
        }
    }

    @Override
    public void uninstall() {
        stopTGTRenewal();
    }
}
