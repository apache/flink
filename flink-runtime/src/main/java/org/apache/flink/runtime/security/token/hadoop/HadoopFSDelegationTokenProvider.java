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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.Clock;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** Delegation token provider for Hadoop filesystems. */
@Internal
public class HadoopFSDelegationTokenProvider implements DelegationTokenProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(HadoopFSDelegationTokenProvider.class);

    private Configuration flinkConfiguration;

    private org.apache.hadoop.conf.Configuration hadoopConfiguration;

    private KerberosLoginProvider kerberosLoginProvider;

    private Optional<Long> tokenRenewalInterval;

    @Override
    public String serviceName() {
        return "hadoopfs";
    }

    @Override
    public void init(Configuration configuration) throws Exception {
        flinkConfiguration = configuration;
        try {
            hadoopConfiguration = HadoopUtils.getHadoopConfiguration(configuration);
            kerberosLoginProvider = new KerberosLoginProvider(configuration);
        } catch (NoClassDefFoundError e) {
            LOG.info(
                    "Hadoop FS is not available (not packaged with this application): {} : \"{}\".",
                    e.getClass().getSimpleName(),
                    e.getMessage());
        }
    }

    @Override
    public boolean delegationTokensRequired() throws Exception {
        /**
         * The general rule how a provider/receiver must behave is the following: The provider and
         * the receiver must be added to the classpath together with all the additionally required
         * dependencies.
         *
         * <p>This null check is required because the Hadoop FS provider is always on classpath but
         * Hadoop FS jars are optional. Such case configuration is not able to be loaded. This
         * construct is intended to be removed when HBase provider/receiver pair can be externalized
         * (namely if a provider/receiver throws an exception then workload must be stopped).
         */
        if (hadoopConfiguration == null) {
            LOG.debug(
                    "Hadoop FS is not available (not packaged with this application), hence no "
                            + "tokens will be acquired.");
            return false;
        }
        return HadoopUtils.isKerberosSecurityEnabled(UserGroupInformation.getCurrentUser())
                && kerberosLoginProvider.isLoginPossible(false);
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        UserGroupInformation freshUGI = kerberosLoginProvider.doLoginAndReturnUGI();
        return freshUGI.doAs(
                (PrivilegedExceptionAction<ObtainedDelegationTokens>)
                        () -> {
                            Credentials credentials = new Credentials();
                            Clock clock = Clock.systemDefaultZone();
                            Set<FileSystem> fileSystemsToAccess = getFileSystemsToAccess();

                            obtainDelegationTokens(getRenewer(), fileSystemsToAccess, credentials);

                            // Get the token renewal interval if it is not set. It will be
                            // called
                            // only once.
                            if (tokenRenewalInterval == null) {
                                tokenRenewalInterval =
                                        getTokenRenewalInterval(clock, fileSystemsToAccess);
                            }
                            Optional<Long> validUntil =
                                    tokenRenewalInterval.flatMap(
                                            interval ->
                                                    getTokenRenewalDate(
                                                            clock, credentials, interval));
                            return new ObtainedDelegationTokens(
                                    HadoopDelegationTokenConverter.serialize(credentials),
                                    validUntil);
                        });
    }

    @VisibleForTesting
    @Nullable
    String getRenewer() {
        return flinkConfiguration.getString(
                String.format("security.kerberos.token.provider.%s.renewer", serviceName()), null);
    }

    private Set<FileSystem> getFileSystemsToAccess() throws IOException {
        Set<FileSystem> result = new HashSet<>();

        // Default filesystem
        FileSystem defaultFileSystem = FileSystem.get(hadoopConfiguration);
        LOG.debug(
                "Adding Hadoop default filesystem to file systems to access {}", defaultFileSystem);
        result.add(defaultFileSystem);
        LOG.debug("Hadoop default filesystem added to file systems to access successfully");

        // Additional filesystems
        ConfigUtils.decodeListFromConfig(
                        flinkConfiguration,
                        SecurityOptions.KERBEROS_HADOOP_FILESYSTEMS_TO_ACCESS,
                        Path::new)
                .forEach(
                        path -> {
                            try {
                                LOG.debug(
                                        "Adding path's filesystem to file systems to access {}",
                                        path);
                                result.add(path.getFileSystem(hadoopConfiguration));
                                LOG.debug(
                                        "Path's filesystem added to file systems to access successfully");
                            } catch (IOException e) {
                                LOG.error("Failed to get filesystem for {}", path, e);
                                throw new FlinkRuntimeException(e);
                            }
                        });

        // YARN staging dir
        if (flinkConfiguration
                .getString(DeploymentOptions.TARGET, "")
                .toLowerCase()
                .contains("yarn")) {
            LOG.debug("Running on YARN, trying to add staging directory to file systems to access");
            String yarnStagingDirectory =
                    flinkConfiguration.getString("yarn.staging-directory", "");
            if (!StringUtils.isBlank(yarnStagingDirectory)) {
                LOG.debug(
                        "Adding staging directory to file systems to access {}",
                        yarnStagingDirectory);
                result.add(new Path(yarnStagingDirectory).getFileSystem(hadoopConfiguration));
                LOG.debug("Staging directory added to file systems to access successfully");
            } else {
                LOG.debug(
                        "Staging directory is not set or empty so not added to file systems to access");
            }
        }

        return result;
    }

    protected void obtainDelegationTokens(
            @Nullable String renewer,
            Set<FileSystem> fileSystemsToAccess,
            Credentials credentials) {
        fileSystemsToAccess.forEach(
                fs -> {
                    try {
                        LOG.debug("Obtaining delegation token for {} with renewer {}", fs, renewer);
                        fs.addDelegationTokens(renewer, credentials);
                        LOG.debug("Delegation obtained successfully");
                    } catch (Exception e) {
                        LOG.error("Failed to obtain delegation token for {}", fs, e);
                        throw new FlinkRuntimeException(e);
                    }
                });
    }

    @VisibleForTesting
    Optional<Long> getTokenRenewalInterval(Clock clock, Set<FileSystem> fileSystemsToAccess)
            throws IOException {
        // We cannot use the tokens generated with renewer yarn
        // Trying to renew those will fail with an access control issue
        // So create new tokens with the logged in user as renewer
        String renewer = UserGroupInformation.getCurrentUser().getUserName();

        Credentials credentials = new Credentials();
        obtainDelegationTokens(renewer, fileSystemsToAccess, credentials);

        Optional<Long> result =
                credentials.getAllTokens().stream()
                        .filter(
                                t -> {
                                    try {
                                        return t.decodeIdentifier()
                                                instanceof AbstractDelegationTokenIdentifier;
                                    } catch (IOException e) {
                                        throw new FlinkRuntimeException(e);
                                    }
                                })
                        .map(
                                t -> {
                                    try {
                                        long newExpiration = t.renew(hadoopConfiguration);
                                        AbstractDelegationTokenIdentifier identifier =
                                                (AbstractDelegationTokenIdentifier)
                                                        t.decodeIdentifier();
                                        String tokenKind = t.getKind().toString();
                                        long interval =
                                                newExpiration
                                                        - getIssueDate(
                                                                clock, tokenKind, identifier);
                                        LOG.debug(
                                                "Renewal interval is {} for token {}",
                                                interval,
                                                tokenKind);
                                        return interval;
                                    } catch (Exception e) {
                                        throw new FlinkRuntimeException(e);
                                    }
                                })
                        .min(Long::compare);

        LOG.debug("Global renewal interval is {}", result);

        return result;
    }

    @VisibleForTesting
    Optional<Long> getTokenRenewalDate(Clock clock, Credentials credentials, long renewalInterval) {
        if (renewalInterval < 0) {
            LOG.debug("Negative renewal interval so no renewal date is calculated");
            return Optional.empty();
        }

        Optional<Long> result =
                credentials.getAllTokens().stream()
                        .filter(
                                t -> {
                                    try {
                                        return t.decodeIdentifier()
                                                instanceof AbstractDelegationTokenIdentifier;
                                    } catch (IOException e) {
                                        throw new FlinkRuntimeException(e);
                                    }
                                })
                        .map(
                                t -> {
                                    try {
                                        AbstractDelegationTokenIdentifier identifier =
                                                (AbstractDelegationTokenIdentifier)
                                                        t.decodeIdentifier();
                                        String tokenKind = t.getKind().toString();
                                        long date =
                                                getIssueDate(clock, tokenKind, identifier)
                                                        + renewalInterval;
                                        LOG.debug(
                                                "Renewal date is {} for token {}", date, tokenKind);
                                        return date;
                                    } catch (Exception e) {
                                        throw new FlinkRuntimeException(e);
                                    }
                                })
                        .min(Long::compare);

        LOG.debug("Global renewal date is {}", result);

        return result;
    }

    @VisibleForTesting
    long getIssueDate(Clock clock, String tokenKind, AbstractDelegationTokenIdentifier identifier) {
        long now = clock.millis();
        long issueDate = identifier.getIssueDate();

        if (issueDate > now) {
            LOG.warn(
                    "Token {} has set up issue date later than current time. (provided: "
                            + "{} / current timestamp: {}) Please make sure clocks are in sync between "
                            + "machines. If the issue is not a clock mismatch, consult token implementor to check "
                            + "whether issue date is valid.",
                    tokenKind,
                    issueDate,
                    now);
            return issueDate;
        } else if (issueDate > 0) {
            return issueDate;
        } else {
            LOG.warn(
                    "Token {} has not set up issue date properly. (provided: {}) "
                            + "Using current timestamp ({}) as issue date instead. Consult token implementor to fix "
                            + "the behavior.",
                    tokenKind,
                    issueDate,
                    now);
            return now;
        }
    }
}
