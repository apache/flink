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

package org.apache.flink.runtime.security.token;

import org.apache.flink.runtime.security.SecurityConfiguration;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** It checks whether kerberos credentials can be renewed. */
public class KerberosRenewalPossibleProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(KerberosRenewalPossibleProvider.class);

    private final SecurityConfiguration securityConfiguration;

    public KerberosRenewalPossibleProvider(SecurityConfiguration securityConfiguration) {
        this.securityConfiguration =
                checkNotNull(
                        securityConfiguration, "Flink security configuration must not be null");
    }

    public boolean isRenewalPossible() throws IOException {
        if (!StringUtils.isBlank(securityConfiguration.getKeytab())
                && !StringUtils.isBlank(securityConfiguration.getPrincipal())) {
            LOG.debug("Login from keytab is possible");
            return true;
        }
        LOG.debug("Login from keytab is NOT possible");

        if (securityConfiguration.useTicketCache() && hasCurrentUserCredentials()) {
            LOG.debug("Login from ticket cache is possible");
            return true;
        }
        LOG.debug("Login from ticket cache is NOT possible");

        return false;
    }

    protected boolean hasCurrentUserCredentials() throws IOException {
        return UserGroupInformation.getCurrentUser().hasKerberosCredentials();
    }
}
