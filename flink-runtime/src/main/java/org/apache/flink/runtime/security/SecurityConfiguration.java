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

package org.apache.flink.runtime.security;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.configuration.SecurityOptions.KERBEROS_RELOGIN_PERIOD;
import static org.apache.flink.configuration.SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES;
import static org.apache.flink.configuration.SecurityOptions.SECURITY_MODULE_FACTORY_CLASSES;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The global security configuration.
 *
 * <p>See {@link SecurityOptions} for corresponding configuration options.
 */
public class SecurityConfiguration {

    private static final String KERBEROS_CONFIG_ERROR_PREFIX =
            "Kerberos login configuration is invalid: ";

    private final List<String> securityContextFactory;

    private final List<String> securityModuleFactories;

    private final Configuration flinkConfig;

    private final boolean isZkSaslDisable;

    private final boolean useTicketCache;

    private final Duration tgtRenewalPeriod;

    private final String keytab;

    private final String principal;

    private final List<String> loginContextNames;

    private final String zkServiceName;

    private final String zkLoginContextName;

    /**
     * Create a security configuration from the global configuration.
     *
     * @param flinkConf the Flink global configuration.
     */
    public SecurityConfiguration(Configuration flinkConf) {
        this(
                flinkConf,
                flinkConf.get(SECURITY_CONTEXT_FACTORY_CLASSES),
                flinkConf.get(SECURITY_MODULE_FACTORY_CLASSES));
    }

    /**
     * Create a security configuration from the global configuration.
     *
     * @param flinkConf the Flink global configuration.
     * @param securityModuleFactories the security modules to apply.
     */
    public SecurityConfiguration(
            Configuration flinkConf,
            List<String> securityContextFactory,
            List<String> securityModuleFactories) {
        this.flinkConfig = checkNotNull(flinkConf);
        this.isZkSaslDisable = flinkConf.getBoolean(SecurityOptions.ZOOKEEPER_SASL_DISABLE);
        this.keytab = flinkConf.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB);
        this.principal = flinkConf.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL);
        this.useTicketCache = flinkConf.getBoolean(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);
        this.tgtRenewalPeriod = flinkConf.get(KERBEROS_RELOGIN_PERIOD);
        this.loginContextNames =
                parseList(flinkConf.getString(SecurityOptions.KERBEROS_LOGIN_CONTEXTS));
        this.zkServiceName = flinkConf.getString(SecurityOptions.ZOOKEEPER_SASL_SERVICE_NAME);
        this.zkLoginContextName =
                flinkConf.getString(SecurityOptions.ZOOKEEPER_SASL_LOGIN_CONTEXT_NAME);
        this.securityModuleFactories = Collections.unmodifiableList(securityModuleFactories);
        this.securityContextFactory = securityContextFactory;
        validate();
    }

    public boolean isZkSaslDisable() {
        return isZkSaslDisable;
    }

    public String getKeytab() {
        return keytab;
    }

    public String getPrincipal() {
        return principal;
    }

    public boolean useTicketCache() {
        return useTicketCache;
    }

    public Duration getTgtRenewalPeriod() {
        return tgtRenewalPeriod;
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    public List<String> getSecurityContextFactories() {
        return securityContextFactory;
    }

    public List<String> getSecurityModuleFactories() {
        return securityModuleFactories;
    }

    public List<String> getLoginContextNames() {
        return loginContextNames;
    }

    public String getZooKeeperServiceName() {
        return zkServiceName;
    }

    public String getZooKeeperLoginContextName() {
        return zkLoginContextName;
    }

    private void validate() {
        if (StringUtils.isBlank(keytab) != StringUtils.isBlank(principal)) {
            throw new IllegalConfigurationException(
                    KERBEROS_CONFIG_ERROR_PREFIX
                            + "either both keytab and principal must be defined, or neither.");
        }

        if (!StringUtils.isBlank(keytab)) {
            File keytabFile = new File(keytab);
            if (!keytabFile.exists() || !keytabFile.isFile()) {
                throw new IllegalConfigurationException(
                        KERBEROS_CONFIG_ERROR_PREFIX + "keytab [" + keytab + "] doesn't exist!");
            } else if (!keytabFile.canRead()) {
                throw new IllegalConfigurationException(
                        KERBEROS_CONFIG_ERROR_PREFIX + "keytab [" + keytab + "] is unreadable!");
            }
        }
    }

    private static List<String> parseList(String value) {
        if (value == null || value.isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.asList(value.trim().replaceAll("(\\s*,+\\s*)+", ",").split(","));
    }
}
