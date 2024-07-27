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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link SecurityOptions}. */
class SecurityOptionsTest {

    /** Tests whether activation of internal / REST SSL evaluates the config flags correctly. */
    @SuppressWarnings("deprecation")
    @Test
    void checkEnableSSL() {
        // backwards compatibility
        Configuration oldConf = new Configuration();
        oldConf.set(SecurityOptions.SSL_ENABLED, true);
        assertThat(SecurityOptions.isInternalSSLEnabled(oldConf)).isTrue();
        assertThat(SecurityOptions.isRestSSLEnabled(oldConf)).isTrue();

        // new options take precedence
        Configuration newOptions = new Configuration();
        newOptions.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        newOptions.set(SecurityOptions.SSL_REST_ENABLED, false);
        assertThat(SecurityOptions.isInternalSSLEnabled(newOptions)).isTrue();
        assertThat(SecurityOptions.isRestSSLEnabled(newOptions)).isFalse();

        // new options take precedence
        Configuration precedence = new Configuration();
        precedence.set(SecurityOptions.SSL_ENABLED, true);
        precedence.set(SecurityOptions.SSL_INTERNAL_ENABLED, false);
        precedence.set(SecurityOptions.SSL_REST_ENABLED, false);
        assertThat(SecurityOptions.isInternalSSLEnabled(precedence)).isFalse();
        assertThat(SecurityOptions.isRestSSLEnabled(precedence)).isFalse();
    }

    /**
     * Tests whether activation of REST mutual SSL authentication evaluates the config flags
     * correctly.
     */
    @Test
    void checkEnableRestSSLAuthentication() {
        // SSL has to be enabled
        Configuration noSSLOptions = new Configuration();
        noSSLOptions.set(SecurityOptions.SSL_REST_ENABLED, false);
        noSSLOptions.set(SecurityOptions.SSL_REST_AUTHENTICATION_ENABLED, true);
        assertThat(SecurityOptions.isRestSSLAuthenticationEnabled(noSSLOptions)).isFalse();

        // authentication is disabled by default
        Configuration defaultOptions = new Configuration();
        defaultOptions.set(SecurityOptions.SSL_REST_ENABLED, true);
        assertThat(SecurityOptions.isRestSSLAuthenticationEnabled(defaultOptions)).isFalse();

        Configuration options = new Configuration();
        options.set(SecurityOptions.SSL_REST_ENABLED, true);
        options.set(SecurityOptions.SSL_REST_AUTHENTICATION_ENABLED, true);
        assertThat(SecurityOptions.isRestSSLAuthenticationEnabled(options)).isTrue();
    }
}
