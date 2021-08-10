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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Permission;

/**
 * {@code FlinkSecurityManager} to control certain behaviors that can be captured by Java system
 * security manager. It can be used to control unexpected user behaviors that potentially impact
 * cluster availability, for example, it can warn or prevent user code from terminating JVM by
 * System.exit or halt by logging or throwing an exception. This does not necessarily prevent
 * malicious users who try to tweak security manager on their own, but more for being dependable
 * against user mistakes by gracefully handling them informing users rather than causing silent
 * unavailability.
 */
public class FlinkSecurityManager extends SecurityManager {

    static final Logger LOG = LoggerFactory.getLogger(FlinkSecurityManager.class);

    /**
     * Security manager reference lastly set to system's security manager by public API. As system
     * security manager can be reset with another but still chain-called into this manager properly,
     * this reference may not be referenced by System.getSecurityManager, but we still need to
     * control runtime check behaviors such as monitoring exit from user code.
     */
    private static FlinkSecurityManager flinkSecurityManager;

    private final SecurityManager originalSecurityManager;
    private final ThreadLocal<Boolean> monitorUserSystemExit = new InheritableThreadLocal<>();
    private final ClusterOptions.UserSystemExitMode userSystemExitMode;

    private final boolean haltOnSystemExit;

    @VisibleForTesting
    FlinkSecurityManager(
            ClusterOptions.UserSystemExitMode userSystemExitMode, boolean haltOnSystemExit) {
        this(userSystemExitMode, haltOnSystemExit, System.getSecurityManager());
    }

    @VisibleForTesting
    FlinkSecurityManager(
            ClusterOptions.UserSystemExitMode userSystemExitMode,
            boolean haltOnSystemExit,
            SecurityManager originalSecurityManager) {
        this.userSystemExitMode = Preconditions.checkNotNull(userSystemExitMode);
        this.haltOnSystemExit = haltOnSystemExit;
        this.originalSecurityManager = originalSecurityManager;
    }

    /**
     * Instantiate FlinkUserSecurityManager from configuration. Return null if no security manager
     * check is needed, so that a caller can skip setting security manager avoiding runtime check
     * cost, if there is no security check set up already. Use {@link #setFromConfiguration} helper,
     * which handles disabled case.
     *
     * @param configuration to instantiate the security manager from
     * @return FlinkUserSecurityManager instantiated based on configuration. Return null if
     *     disabled.
     */
    @VisibleForTesting
    static FlinkSecurityManager fromConfiguration(Configuration configuration) {
        final ClusterOptions.UserSystemExitMode userSystemExitMode =
                configuration.get(ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT);

        boolean haltOnSystemExit = configuration.get(ClusterOptions.HALT_ON_FATAL_ERROR);

        // If no check is needed, return null so that caller can avoid setting security manager not
        // to incur any runtime cost.
        if (userSystemExitMode == ClusterOptions.UserSystemExitMode.DISABLED && !haltOnSystemExit) {
            return null;
        }
        LOG.info(
                "FlinkSecurityManager is created with {} user system exit mode and {} exit",
                userSystemExitMode,
                haltOnSystemExit ? "forceful" : "graceful");
        // Add more configuration parameters that need user security manager (currently only for
        // system exit).
        return new FlinkSecurityManager(userSystemExitMode, haltOnSystemExit);
    }

    public static void setFromConfiguration(Configuration configuration) {
        final FlinkSecurityManager flinkSecurityManager =
                FlinkSecurityManager.fromConfiguration(configuration);
        if (flinkSecurityManager != null) {
            try {
                System.setSecurityManager(flinkSecurityManager);
            } catch (Exception e) {
                throw new IllegalConfigurationException(
                        String.format(
                                "Could not register security manager due to no permission to "
                                        + "set a SecurityManager. Either update your existing "
                                        + "SecurityManager to allow the permission or do not use "
                                        + "security manager features (e.g., '%s: %s', '%s: %s')",
                                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT.key(),
                                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT.defaultValue(),
                                ClusterOptions.HALT_ON_FATAL_ERROR.key(),
                                ClusterOptions.HALT_ON_FATAL_ERROR.defaultValue(),
                                e));
            }
        }
        FlinkSecurityManager.flinkSecurityManager = flinkSecurityManager;
    }

    public static void monitorUserSystemExitForCurrentThread() {
        if (flinkSecurityManager != null) {
            flinkSecurityManager.monitorUserSystemExit();
        }
    }

    public static void unmonitorUserSystemExitForCurrentThread() {
        if (flinkSecurityManager != null) {
            flinkSecurityManager.unmonitorUserSystemExit();
        }
    }

    @Override
    public void checkPermission(Permission perm) {
        if (originalSecurityManager != null) {
            originalSecurityManager.checkPermission(perm);
        }
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if (originalSecurityManager != null) {
            originalSecurityManager.checkPermission(perm, context);
        }
    }

    @Override
    public void checkExit(int status) {
        if (userSystemExitMonitored()) {
            switch (userSystemExitMode) {
                case DISABLED:
                    break;
                case LOG:
                    // Add exception trace log to help users to debug where exit came from.
                    LOG.warn(
                            "Exiting JVM with status {} is monitored: The system will exit due to this call.",
                            status,
                            new UserSystemExitException());
                    break;
                case THROW:
                    throw new UserSystemExitException();
                default:
                    // Must not happen if exhaustively handling all modes above. Logging as being
                    // already at exit path.
                    LOG.warn("No valid check exit mode configured: {}", userSystemExitMode);
            }
        }
        // As this security manager is current at outer most of the chain and it has exit guard
        // option, invoke inner security manager here after passing guard checking above, if any.
        if (originalSecurityManager != null) {
            originalSecurityManager.checkExit(status);
        }
        // At this point, exit is determined. Halt if defined, otherwise check ended, JVM will call
        // System.exit
        if (haltOnSystemExit) {
            Runtime.getRuntime().halt(status);
        }
    }

    @VisibleForTesting
    void monitorUserSystemExit() {
        monitorUserSystemExit.set(true);
    }

    @VisibleForTesting
    void unmonitorUserSystemExit() {
        monitorUserSystemExit.set(false);
    }

    @VisibleForTesting
    boolean userSystemExitMonitored() {
        return Boolean.TRUE.equals(monitorUserSystemExit.get());
    }

    /**
     * Use this method to circumvent the configured {@link FlinkSecurityManager} behavior, ensuring
     * that the current JVM process will always stop via System.exit() or
     * Runtime.getRuntime().halt().
     */
    public static void forceProcessExit(int exitCode) {
        // Unset ourselves to allow exiting in any case.
        System.setSecurityManager(null);
        if (flinkSecurityManager != null && flinkSecurityManager.haltOnSystemExit) {
            Runtime.getRuntime().halt(exitCode);
        } else {
            System.exit(exitCode);
        }
    }
}
