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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.contexts.HadoopSecurityContext;
import org.apache.flink.test.util.SecureTestEnvironment;
import org.apache.flink.test.util.TestingSecurityContext;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.util.TestHadoopModuleFactory;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * An extension of the {@link YARNSessionFIFOITCase} that runs the tests in a secured YARN cluster.
 */
class YARNSessionFIFOSecuredITCase extends YARNSessionFIFOITCase {

    private static final Logger log = LoggerFactory.getLogger(YARNSessionFIFOSecuredITCase.class);

    @BeforeAll
    public static void setup() {

        log.info("starting secure cluster environment for testing");

        YARN_CONFIGURATION.setClass(
                YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 768);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
        YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-fifo-secured");

        SecureTestEnvironment.prepare(tmp);

        populateYarnSecureConfigurations(
                YARN_CONFIGURATION,
                SecureTestEnvironment.getHadoopServicePrincipal(),
                SecureTestEnvironment.getTestKeytab());

        Configuration flinkConfig = new Configuration();
        flinkConfig.setString(
                SecurityOptions.KERBEROS_LOGIN_KEYTAB, SecureTestEnvironment.getTestKeytab());
        flinkConfig.setString(
                SecurityOptions.KERBEROS_LOGIN_PRINCIPAL,
                SecureTestEnvironment.getHadoopServicePrincipal());

        // Setting customized security module class.
        TestHadoopModuleFactory.hadoopConfiguration = YARN_CONFIGURATION;
        flinkConfig.set(
                SecurityOptions.SECURITY_MODULE_FACTORY_CLASSES,
                Collections.singletonList("org.apache.flink.yarn.util.TestHadoopModuleFactory"));
        flinkConfig.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Collections.singletonList(
                        "org.apache.flink.yarn.util.TestHadoopSecurityContextFactory"));

        SecurityConfiguration securityConfig = new SecurityConfiguration(flinkConfig);

        try {
            TestingSecurityContext.install(
                    securityConfig, SecureTestEnvironment.getClientSecurityConfigurationMap());

            // This is needed to ensure that SecurityUtils are run within a ugi.doAs section
            // Since we already logged in here in @BeforeClass, even a no-op security context will
            // still work.
            assertThat(SecurityUtils.getInstalledContext())
                    .isInstanceOf(HadoopSecurityContext.class);
            SecurityUtils.getInstalledContext()
                    .runSecured(
                            () -> {
                                startYARNSecureMode(
                                        YARN_CONFIGURATION,
                                        SecureTestEnvironment.getHadoopServicePrincipal(),
                                        SecureTestEnvironment.getTestKeytab());
                                return null;
                            });

        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception occurred while setting up secure test context. Reason: {}", e);
        }
    }

    @AfterAll
    static void teardownSecureCluster() {
        log.info("tearing down secure cluster environment");
        SecureTestEnvironment.cleanup();
    }

    @Test
    void testDetachedModeSecureWithPreInstallKeytab() throws Exception {
        runTest(
                () -> {
                    Map<String, String> securityProperties = new HashMap<>();
                    if (SecureTestEnvironment.getTestKeytab() != null) {
                        // client login keytab
                        securityProperties.put(
                                SecurityOptions.KERBEROS_LOGIN_KEYTAB.key(),
                                SecureTestEnvironment.getTestKeytab());
                        // pre-install Yarn local keytab, since both reuse the same temporary folder
                        // "tmp"
                        securityProperties.put(
                                YarnConfigOptions.LOCALIZED_KEYTAB_PATH.key(),
                                SecureTestEnvironment.getTestKeytab());
                        // unset keytab localization
                        securityProperties.put(YarnConfigOptions.SHIP_LOCAL_KEYTAB.key(), "false");
                    }
                    if (SecureTestEnvironment.getHadoopServicePrincipal() != null) {
                        securityProperties.put(
                                SecurityOptions.KERBEROS_LOGIN_PRINCIPAL.key(),
                                SecureTestEnvironment.getHadoopServicePrincipal());
                    }
                    final ApplicationId applicationId =
                            runDetachedModeTest(securityProperties, VIEW_ACLS, MODIFY_ACLS);
                    verifyResultContainsKerberosKeytab(applicationId, VIEW_ACLS, MODIFY_ACLS);
                });
    }

    @Test
    @Override
    void testDetachedMode() throws Exception {
        runTest(
                () -> {
                    Map<String, String> securityProperties = new HashMap<>();
                    if (SecureTestEnvironment.getTestKeytab() != null) {
                        securityProperties.put(
                                SecurityOptions.KERBEROS_LOGIN_KEYTAB.key(),
                                SecureTestEnvironment.getTestKeytab());
                    }
                    if (SecureTestEnvironment.getHadoopServicePrincipal() != null) {
                        securityProperties.put(
                                SecurityOptions.KERBEROS_LOGIN_PRINCIPAL.key(),
                                SecureTestEnvironment.getHadoopServicePrincipal());
                    }
                    final ApplicationId applicationId =
                            runDetachedModeTest(securityProperties, VIEW_ACLS, MODIFY_ACLS);
                    verifyResultContainsKerberosKeytab(applicationId, VIEW_ACLS, MODIFY_ACLS);
                    final ApplicationId applicationIdWithWildcard =
                            runDetachedModeTest(
                                    securityProperties,
                                    VIEW_ACLS_WITH_WILDCARD,
                                    MODIFY_ACLS_WITH_WILDCARD);
                    verifyResultContainsKerberosKeytab(
                            applicationIdWithWildcard, WILDCARD, WILDCARD);
                });
    }

    private static void verifyResultContainsKerberosKeytab(
            ApplicationId applicationId, String viewAcls, String modifyAcls) throws Exception {
        final String[] mustHave = {"Login successful for user", "using keytab file"};
        final boolean jobManagerRunsWithKerberos =
                verifyStringsInNamedLogFiles(mustHave, applicationId, "jobmanager.log");
        final boolean taskManagerRunsWithKerberos =
                verifyStringsInNamedLogFiles(mustHave, applicationId, "taskmanager.log");

        assertThat(jobManagerRunsWithKerberos && taskManagerRunsWithKerberos).isTrue();

        final List<String> amRMTokens =
                Lists.newArrayList(AMRMTokenIdentifier.KIND_NAME.toString());
        final String jobmanagerContainerId = getContainerIdByLogName("jobmanager.log");
        final String taskmanagerContainerId = getContainerIdByLogName("taskmanager.log");
        final boolean jobmanagerWithAmRmToken =
                verifyTokenKindInContainerCredentials(amRMTokens, jobmanagerContainerId);
        final boolean taskmanagerWithAmRmToken =
                verifyTokenKindInContainerCredentials(amRMTokens, taskmanagerContainerId);

        assertThat(jobmanagerWithAmRmToken).isTrue();
        assertThat(taskmanagerWithAmRmToken).isFalse();

        getRunningContainersAcls()
                .forEach(
                        (k, acls) -> {
                            Assert.assertEquals(viewAcls, acls.get(ApplicationAccessType.VIEW_APP));
                            Assert.assertEquals(
                                    modifyAcls, acls.get(ApplicationAccessType.MODIFY_APP));
                        });
    }

    /* For secure cluster testing, it is enough to run only one test and override below test methods
     * to keep the overall build time minimal
     */
    @Override
    public void testQueryCluster() {}

    /**
     * Returns a map of the application ACLs for all running containers in the YARN cluster. The map
     * is keyed by container ID, and the values are maps of application access types to
     * corresponding ACL strings.
     *
     * @return a map of the application ACLs for all running containers in the YARN cluster
     */
    private static Map<ContainerId, Map<ApplicationAccessType, String>> getRunningContainersAcls() {
        return nodeManagersStream()
                .flatMap(toContainersStream())
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, entry -> getApplicationACLs(entry.getValue())));
    }

    /**
     * Returns a stream of NodeManager instances in the YARN cluster.
     *
     * @return a stream of NodeManager instances in the YARN cluster
     */
    private static Stream<NodeManager> nodeManagersStream() {
        return IntStream.range(0, NUM_NODEMANAGERS).mapToObj(i -> yarnCluster.getNodeManager(i));
    }

    /**
     * Returns a function that maps a NodeManager to a stream of container ID-container pairs. The
     * resulting stream contains all containers currently running on the NodeManager.
     *
     * @return a function that maps a NodeManager to a stream of container ID-container pairs
     */
    private static Function<NodeManager, Stream<Map.Entry<ContainerId, Container>>>
            toContainersStream() {
        return nodeManager -> nodeManager.getNMContext().getContainers().entrySet().stream();
    }

    /**
     * Returns a map of the application ACLs for the given container. The map is keyed by
     * application access type, and the values are the corresponding ACL strings.
     *
     * @param container the container to retrieve the ACLs for
     * @return a map of the application ACLs for the given container
     */
    private static Map<ApplicationAccessType, String> getApplicationACLs(
            final Container container) {
        return container.getLaunchContext().getApplicationACLs();
    }
}
