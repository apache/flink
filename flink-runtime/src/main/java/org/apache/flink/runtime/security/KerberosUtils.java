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

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides vendor-specific Kerberos {@link AppConfigurationEntry} instances.
 *
 * <p>The implementation is inspired from Hadoop UGI class.
 */
@Internal
public class KerberosUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosUtils.class);

    private static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");

    private static final boolean IBM_JAVA;

    private static final Map<String, String> debugOptions = new HashMap<>();

    private static final Map<String, String> kerberosCacheOptions = new HashMap<>();

    private static final AppConfigurationEntry userKerberosAce;

    /* Return the Kerberos login module name */
    public static String getKrb5LoginModuleName() {
        return System.getProperty("java.vendor").contains("IBM")
                ? "com.ibm.security.auth.module.Krb5LoginModule"
                : "com.sun.security.auth.module.Krb5LoginModule";
    }

    static {
        IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");

        if (LOG.isDebugEnabled()) {
            debugOptions.put("debug", "true");
        }

        if (IBM_JAVA) {
            kerberosCacheOptions.put("useDefaultCcache", "true");
        } else {
            kerberosCacheOptions.put("doNotPrompt", "true");
            kerberosCacheOptions.put("useTicketCache", "true");
        }

        String ticketCache = System.getenv("KRB5CCNAME");
        if (ticketCache != null) {
            if (IBM_JAVA) {
                System.setProperty("KRB5CCNAME", ticketCache);
            } else {
                kerberosCacheOptions.put("ticketCache", ticketCache);
            }
        }

        kerberosCacheOptions.put("renewTGT", "true");
        kerberosCacheOptions.putAll(debugOptions);

        userKerberosAce =
                new AppConfigurationEntry(
                        getKrb5LoginModuleName(),
                        AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                        kerberosCacheOptions);
    }

    public static AppConfigurationEntry ticketCacheEntry() {
        return userKerberosAce;
    }

    public static AppConfigurationEntry keytabEntry(String keytab, String principal) {

        checkNotNull(keytab, "keytab");
        checkNotNull(principal, "principal");

        Map<String, String> keytabKerberosOptions = new HashMap<>();

        if (IBM_JAVA) {
            keytabKerberosOptions.put("useKeytab", prependFileUri(keytab));
            keytabKerberosOptions.put("credsType", "both");
        } else {
            keytabKerberosOptions.put("keyTab", keytab);
            keytabKerberosOptions.put("doNotPrompt", "true");
            keytabKerberosOptions.put("useKeyTab", "true");
            keytabKerberosOptions.put("storeKey", "true");
        }

        keytabKerberosOptions.put("principal", principal);
        keytabKerberosOptions.put("refreshKrb5Config", "true");
        keytabKerberosOptions.putAll(debugOptions);

        AppConfigurationEntry keytabKerberosAce =
                new AppConfigurationEntry(
                        getKrb5LoginModuleName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        keytabKerberosOptions);

        return keytabKerberosAce;
    }

    private static String prependFileUri(String keytabPath) {
        File f = new File(keytabPath);
        return f.toURI().toString();
    }
}
