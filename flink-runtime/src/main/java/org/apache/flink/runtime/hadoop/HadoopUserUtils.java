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

package org.apache.flink.runtime.hadoop;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;

/**
 * Utility class for working with Hadoop user related classes. This should only be used if Hadoop is
 * on the classpath.
 */
public class HadoopUserUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopUserUtils.class);

    public static boolean isProxyUser(UserGroupInformation ugi) {
        return ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.PROXY;
    }

    public static boolean hasUserKerberosAuthMethod(UserGroupInformation ugi) {
        return UserGroupInformation.isSecurityEnabled()
                && ugi.getAuthenticationMethod()
                        == UserGroupInformation.AuthenticationMethod.KERBEROS;
    }

    public static long getIssueDate(
            Clock clock, String tokenKind, AbstractDelegationTokenIdentifier identifier) {
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
