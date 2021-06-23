/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link SecurityOptions}. */
public class SecurityOptionsTest extends TestLogger {

    /** Tests whether activation of internal / REST SSL evaluates the config flags correctly. */
    @SuppressWarnings("deprecation")
    @Test
    public void checkEnableSSL() {
        // backwards compatibility
        Configuration oldConf = new Configuration();
        oldConf.setBoolean(SecurityOptions.SSL_ENABLED, true);
        assertTrue(SecurityOptions.isInternalSSLEnabled(oldConf));
        assertTrue(SecurityOptions.isRestSSLEnabled(oldConf));

        // new options take precedence
        Configuration newOptions = new Configuration();
        newOptions.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        newOptions.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);
        assertTrue(SecurityOptions.isInternalSSLEnabled(newOptions));
        assertFalse(SecurityOptions.isRestSSLEnabled(newOptions));

        // new options take precedence
        Configuration precedence = new Configuration();
        precedence.setBoolean(SecurityOptions.SSL_ENABLED, true);
        precedence.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, false);
        precedence.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);
        assertFalse(SecurityOptions.isInternalSSLEnabled(precedence));
        assertFalse(SecurityOptions.isRestSSLEnabled(precedence));
    }

    /**
     * Tests whether activation of REST mutual SSL authentication evaluates the config flags
     * correctly.
     */
    @Test
    public void checkEnableRestSSLAuthentication() {
        // SSL has to be enabled
        Configuration noSSLOptions = new Configuration();
        noSSLOptions.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);
        noSSLOptions.setBoolean(SecurityOptions.SSL_REST_AUTHENTICATION_ENABLED, true);
        assertFalse(SecurityOptions.isRestSSLAuthenticationEnabled(noSSLOptions));

        // authentication is disabled by default
        Configuration defaultOptions = new Configuration();
        defaultOptions.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        assertFalse(SecurityOptions.isRestSSLAuthenticationEnabled(defaultOptions));

        Configuration options = new Configuration();
        options.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        options.setBoolean(SecurityOptions.SSL_REST_AUTHENTICATION_ENABLED, true);
        assertTrue(SecurityOptions.isRestSSLAuthenticationEnabled(options));
    }
}
