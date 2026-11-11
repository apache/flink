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
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.security.contexts.TestSecurityContextFactory;
import org.apache.flink.runtime.security.modules.SecurityModule;
import org.apache.flink.runtime.security.modules.TestSecurityModuleFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for idempotent installation in {@link SecurityUtils}. */
class SecurityUtilsInstallTest {

    @AfterEach
    void tearDown() {
        SecurityUtils.uninstall();
    }

    @Test
    void installShouldBeNoOpForEqualSecurityConfiguration() throws Exception {
        final Configuration flinkConf = new Configuration();
        SecurityUtils.install(createSecurityConfiguration(flinkConf));
        final List<SecurityModule> firstModules = SecurityUtils.getInstalledModules();
        final SecurityContext firstContext = SecurityUtils.getInstalledContext();

        // Mirrors the K8s application mode, which only adds pipeline options between installs.
        flinkConf.set(
                PipelineOptions.JARS, Collections.singletonList("local:///opt/flink/job.jar"));
        SecurityUtils.install(createSecurityConfiguration(flinkConf));

        assertThat(SecurityUtils.getInstalledModules()).isSameAs(firstModules);
        assertThat(SecurityUtils.getInstalledContext()).isSameAs(firstContext);
    }

    @Test
    void installShouldReinstallForDifferentSecurityConfiguration() throws Exception {
        final Configuration flinkConf = new Configuration();
        SecurityUtils.install(createSecurityConfiguration(flinkConf));
        final List<SecurityModule> firstModules = SecurityUtils.getInstalledModules();
        final SecurityContext firstContext = SecurityUtils.getInstalledContext();

        flinkConf.set(SecurityOptions.ZOOKEEPER_SASL_DISABLE, true);
        SecurityUtils.install(createSecurityConfiguration(flinkConf));

        assertThat(SecurityUtils.getInstalledModules()).isNotSameAs(firstModules);
        assertThat(SecurityUtils.getInstalledContext()).isNotSameAs(firstContext);
    }

    @Test
    void installShouldReinstallAfterUninstall() throws Exception {
        final Configuration flinkConf = new Configuration();
        SecurityUtils.install(createSecurityConfiguration(flinkConf));
        final List<SecurityModule> firstModules = SecurityUtils.getInstalledModules();

        SecurityUtils.uninstall();
        SecurityUtils.install(createSecurityConfiguration(flinkConf));

        assertThat(SecurityUtils.getInstalledModules()).isNotNull().isNotSameAs(firstModules);
    }

    private static SecurityConfiguration createSecurityConfiguration(Configuration flinkConf) {
        return new SecurityConfiguration(
                flinkConf,
                Collections.singletonList(TestSecurityContextFactory.class.getCanonicalName()),
                Collections.singletonList(TestSecurityModuleFactory.class.getCanonicalName()));
    }
}
