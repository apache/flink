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

package org.apache.flink.core.security;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.security.Permission;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@code FlinkUserSecurityManager}. */
class FlinkSecurityManagerTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final int TEST_EXIT_CODE = 123;
    SecurityManager originalSecurityManager;

    @BeforeEach
    void setUp() {
        originalSecurityManager = System.getSecurityManager();
    }

    @AfterEach
    void tearDown() {
        System.setSecurityManager(originalSecurityManager);
    }

    @Test
    void testThrowUserExit() {
        assertThatThrownBy(
                        () -> {
                            FlinkSecurityManager flinkSecurityManager =
                                    new FlinkSecurityManager(
                                            ClusterOptions.UserSystemExitMode.THROW, false);
                            flinkSecurityManager.monitorUserSystemExit();
                            flinkSecurityManager.checkExit(TEST_EXIT_CODE);
                        })
                .isInstanceOf(UserSystemExitException.class);
    }

    @Test
    void testToggleUserExit() {
        FlinkSecurityManager flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.THROW, false);
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
        flinkSecurityManager.monitorUserSystemExit();
        assertThatThrownBy(() -> flinkSecurityManager.checkExit(TEST_EXIT_CODE))
                .isInstanceOf(UserSystemExitException.class);
        flinkSecurityManager.unmonitorUserSystemExit();
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
    }

    @Test
    void testPerThreadThrowUserExit() throws Exception {
        FlinkSecurityManager flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.THROW, false);
        ExecutorService executorService = EXECUTOR_RESOURCE.getExecutor();
        // Async thread test before enabling monitoring ensures it does not throw while prestarting
        // worker thread, which is to be unmonitored and tested after enabling monitoring enabled.
        CompletableFuture<Void> future =
                CompletableFuture.runAsync(
                        () -> flinkSecurityManager.checkExit(TEST_EXIT_CODE), executorService);
        future.get();
        flinkSecurityManager.monitorUserSystemExit();
        assertThatThrownBy(() -> flinkSecurityManager.checkExit(TEST_EXIT_CODE))
                .isInstanceOf(UserSystemExitException.class);
        // This threaded exit should be allowed as thread is not spawned while monitor is enabled.
        future =
                CompletableFuture.runAsync(
                        () -> flinkSecurityManager.checkExit(TEST_EXIT_CODE), executorService);
        future.get();
    }

    @Test
    void testInheritedThrowUserExit() throws Exception {
        FlinkSecurityManager flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.THROW, false);
        flinkSecurityManager.monitorUserSystemExit();
        assertThatThrownBy(() -> flinkSecurityManager.checkExit(TEST_EXIT_CODE))
                .isInstanceOf(UserSystemExitException.class);
        CheckedThread thread =
                new CheckedThread() {
                    @Override
                    public void go() {
                        assertThatThrownBy(() -> flinkSecurityManager.checkExit(TEST_EXIT_CODE))
                                .isInstanceOf(UserSystemExitException.class);
                    }
                };
        thread.start();
        thread.sync();
    }

    @Test
    void testLogUserExit() {
        // Log mode enables monitor but only logging allowing exit, hence not expecting exception.
        // NOTE - Do not specifically test warning logging.
        FlinkSecurityManager flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.LOG, false);
        flinkSecurityManager.monitorUserSystemExit();
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
    }

    @Test
    void testDisabledConfiguration() {
        // Default case (no provided option) - allowing everything, so null security manager is
        // expected.
        Configuration configuration = new Configuration();
        FlinkSecurityManager flinkSecurityManager =
                FlinkSecurityManager.fromConfiguration(configuration);
        assertThat(flinkSecurityManager).isNull();

        // Disabled case (same as default)
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT,
                ClusterOptions.UserSystemExitMode.DISABLED);
        flinkSecurityManager = FlinkSecurityManager.fromConfiguration(configuration);
        assertThat(flinkSecurityManager).isNull();

        // No halt (same as default)
        configuration.set(ClusterOptions.HALT_ON_FATAL_ERROR, false);
        flinkSecurityManager = FlinkSecurityManager.fromConfiguration(configuration);
        assertThat(flinkSecurityManager).isNull();
    }

    @Test
    void testLogConfiguration() {
        // Enabled - log case (logging as warning but allowing exit)
        Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.LOG);
        FlinkSecurityManager flinkSecurityManager =
                FlinkSecurityManager.fromConfiguration(configuration);
        assertThat(flinkSecurityManager).isNotNull();
        assertThat(flinkSecurityManager.userSystemExitMonitored()).isFalse();
        flinkSecurityManager.monitorUserSystemExit();
        assertThat(flinkSecurityManager.userSystemExitMonitored()).isTrue();
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
        flinkSecurityManager.unmonitorUserSystemExit();
        assertThat(flinkSecurityManager.userSystemExitMonitored()).isFalse();
    }

    @Test
    void testThrowConfiguration() {
        // Enabled - throw case (disallowing by throwing exception)
        Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        FlinkSecurityManager flinkSecurityManager =
                FlinkSecurityManager.fromConfiguration(configuration);
        assertThat(flinkSecurityManager).isNotNull();
        assertThat(flinkSecurityManager.userSystemExitMonitored()).isFalse();
        flinkSecurityManager.monitorUserSystemExit();
        assertThat(flinkSecurityManager.userSystemExitMonitored()).isTrue();
        FlinkSecurityManager finalFlinkSecurityManager = flinkSecurityManager;
        assertThatThrownBy(() -> finalFlinkSecurityManager.checkExit(TEST_EXIT_CODE))
                .isInstanceOf(UserSystemExitException.class);
        flinkSecurityManager.unmonitorUserSystemExit();
        assertThat(flinkSecurityManager.userSystemExitMonitored()).isFalse();

        // Test for disabled test to check if exit is still allowed (fromConfiguration gives null
        // since currently
        // there is only one option to have a valid security manager, so test with constructor).
        flinkSecurityManager =
                new FlinkSecurityManager(ClusterOptions.UserSystemExitMode.DISABLED, false);
        flinkSecurityManager.monitorUserSystemExit();
        assertThat(flinkSecurityManager.userSystemExitMonitored()).isTrue();
        flinkSecurityManager.checkExit(TEST_EXIT_CODE);
    }

    @Test
    void testHaltConfiguration() {
        // Halt as forceful shutdown replacing graceful system exit
        Configuration configuration = new Configuration();
        configuration.set(ClusterOptions.HALT_ON_FATAL_ERROR, true);
        FlinkSecurityManager flinkSecurityManager =
                FlinkSecurityManager.fromConfiguration(configuration);
        assertThat(flinkSecurityManager).isNotNull();
    }

    @Test
    void testInvalidConfiguration() {
        assertThatThrownBy(
                        () -> {
                            Configuration configuration = new Configuration();
                            configuration.set(ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, null);
                            FlinkSecurityManager.fromConfiguration(configuration);
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testExistingSecurityManagerRespected() {
        // Don't set the following security manager directly to system, which makes test hang.
        SecurityManager originalSecurityManager =
                new SecurityManager() {
                    @Override
                    public void checkPermission(Permission perm) {
                        throw new SecurityException("not allowed");
                    }
                };
        FlinkSecurityManager flinkSecurityManager =
                new FlinkSecurityManager(
                        ClusterOptions.UserSystemExitMode.DISABLED, false, originalSecurityManager);

        assertThatThrownBy(() -> flinkSecurityManager.checkExit(TEST_EXIT_CODE))
                .isInstanceOf(SecurityException.class)
                .hasMessage("not allowed");
    }

    @Test
    void testRegistrationNotAllowedByExistingSecurityManager() {
        Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);

        System.setSecurityManager(
                new SecurityManager() {

                    private boolean fired;

                    @Override
                    public void checkPermission(Permission perm) {
                        if (!fired && perm.getName().equals("setSecurityManager")) {
                            try {
                                throw new SecurityException("not allowed");
                            } finally {
                                // Allow removing this manager again
                                fired = true;
                            }
                        }
                    }
                });

        assertThatThrownBy(() -> FlinkSecurityManager.setFromConfiguration(configuration))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Could not register security manager");
    }

    @Test
    void testMultiSecurityManagersWithSetFirstAndMonitored() {
        Configuration configuration = new Configuration();

        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        configuration.set(ClusterOptions.HALT_ON_FATAL_ERROR, false);

        FlinkSecurityManager.setFromConfiguration(configuration);

        TestExitSecurityManager newSecurityManager = new TestExitSecurityManager();
        System.setSecurityManager(newSecurityManager);

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        assertThatThrownBy(() -> newSecurityManager.checkExit(TEST_EXIT_CODE))
                .isInstanceOf(UserSystemExitException.class);
        assertThat(newSecurityManager.getExitStatus()).isEqualTo(TEST_EXIT_CODE);
    }

    @Test
    void testMultiSecurityManagersWithSetLastAndMonitored() {
        Configuration configuration = new Configuration();

        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        configuration.set(ClusterOptions.HALT_ON_FATAL_ERROR, false);

        TestExitSecurityManager oldSecurityManager = new TestExitSecurityManager();
        System.setSecurityManager(oldSecurityManager);

        FlinkSecurityManager.setFromConfiguration(configuration);

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        assertThatThrownBy(() -> System.getSecurityManager().checkExit(TEST_EXIT_CODE))
                .isInstanceOf(UserSystemExitException.class);
        assertThat(oldSecurityManager.getExitStatus()).isNull();
    }

    @Test
    void testMultiSecurityManagersWithSetFirstAndUnmonitored() {
        Configuration configuration = new Configuration();

        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        configuration.set(ClusterOptions.HALT_ON_FATAL_ERROR, false);

        FlinkSecurityManager.setFromConfiguration(configuration);

        TestExitSecurityManager newSecurityManager = new TestExitSecurityManager();
        System.setSecurityManager(newSecurityManager);

        newSecurityManager.checkExit(TEST_EXIT_CODE);
        assertThat(newSecurityManager.getExitStatus()).isEqualTo(TEST_EXIT_CODE);
    }

    @Test
    void testMultiSecurityManagersWithSetLastAndUnmonitored() {
        Configuration configuration = new Configuration();

        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        configuration.set(ClusterOptions.HALT_ON_FATAL_ERROR, false);

        TestExitSecurityManager oldSecurityManager = new TestExitSecurityManager();
        System.setSecurityManager(oldSecurityManager);

        FlinkSecurityManager.setFromConfiguration(configuration);

        System.getSecurityManager().checkExit(TEST_EXIT_CODE);
        assertThat(oldSecurityManager.getExitStatus()).isEqualTo(TEST_EXIT_CODE);
    }

    private class TestExitSecurityManager extends SecurityManager {
        private final SecurityManager originalSecurityManager;
        private Integer exitStatus;

        public TestExitSecurityManager() {
            originalSecurityManager = System.getSecurityManager();
        }

        @Override
        public void checkExit(int status) {
            exitStatus = status;
            if (originalSecurityManager != null) {
                originalSecurityManager.checkExit(status);
            }
        }

        @Override
        public void checkPermission(Permission perm) {}

        public Integer getExitStatus() {
            return exitStatus;
        }
    }
}
