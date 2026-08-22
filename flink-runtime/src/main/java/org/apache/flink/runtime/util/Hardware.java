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

package org.apache.flink.runtime.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.OperatingSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Convenience class to extract hardware specifics of the computer executing the running JVM. */
public class Hardware {

    private static final Logger LOG = LoggerFactory.getLogger(Hardware.class);

    private static final String LINUX_MEMORY_INFO_PATH = "/proc/meminfo";

    private static final Pattern LINUX_MEMORY_REGEX =
            Pattern.compile("^MemTotal:\\s*(\\d+)\\s+kB$");

    private static final String CGROUP_V2_CPU_MAX_PATH = "/sys/fs/cgroup/cpu.max";

    private static final String CGROUP_V1_CPU_QUOTA_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
    private static final String CGROUP_V1_CPU_PERIOD_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

    /** Sentinel value used by cgroup v2 in {@code cpu.max} to indicate an unlimited CPU quota. */
    static final String CGROUP_V2_UNLIMITED = "max";

    // ------------------------------------------------------------------------

    /**
     * Gets the number of CPU cores (hardware contexts) that the JVM has access to.
     *
     * <p>This always returns an integer and is intended for call sites that need an {@code int}
     * (e.g. thread-pool sizes). When the fractional value matters (e.g. for display purposes or
     * when performing arithmetic before rounding), use {@link #getNumberCPUCoresAsDouble()}
     * instead.
     *
     * @return The number of CPU cores.
     */
    public static int getNumberCPUCores() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Gets the number of CPU cores available to the JVM as a fractional value.
     *
     * <p>On Linux, this method first attempts to detect a container CPU limit via cgroup files (v2,
     * then v1). If a limit is found, it is returned as-is (e.g. 0.5, 1.5). If no container limit is
     * detected, it falls back to {@link Runtime#availableProcessors()}.
     *
     * <p>Use this method when the fractional value matters, for example when displaying the CPU
     * count in the Web UI or when performing arithmetic before rounding (e.g. {@code 4 * cores}).
     * For call sites that need an integer (e.g. thread pool sizes), use {@link
     * #getNumberCPUCores()} instead.
     *
     * @return The number of CPU cores as a double.
     */
    public static double getNumberCPUCoresAsDouble() {
        double containerLimit = getContainerCpuLimit();
        if (containerLimit > 0) {
            LOG.debug("Using container CPU limit for core count: limit={}", containerLimit);
            return containerLimit;
        }
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Returns the CPU limit of the container as a fractional double by reading Linux cgroup CPU
     * quota and period values.
     *
     * <p>This method attempts to read the CPU limit from cgroup v2 first ({@code
     * /sys/fs/cgroup/cpu.max}), then falls back to cgroup v1 ({@code
     * /sys/fs/cgroup/cpu/cpu.cfs_quota_us} and {@code cpu.cfs_period_us}). The v1 fallback also
     * covers cgroup v2 <em>hybrid</em> mode, where the {@code cpu} controller typically remains
     * mounted under the v1 hierarchy.
     *
     * <p>Examples of return values:
     *
     * <ul>
     *   <li>{@code 0.5} - container limited to half a CPU core
     *   <li>{@code 2.0} - container limited to 2 CPU cores
     *   <li>{@code -1} - not running in a container, no CPU limit set, or unable to read cgroup
     *       files (e.g. non-Linux OS)
     * </ul>
     *
     * @return the container CPU limit as a fractional double, or {@code -1} if no limit is detected
     */
    public static double getContainerCpuLimit() {
        // Try cgroup v2 first
        double limit = getCpuLimitFromCgroupV2(Paths.get(CGROUP_V2_CPU_MAX_PATH));
        if (limit > 0) {
            return limit;
        }

        // Fall back to cgroup v1 (also covers v2 hybrid mode)
        limit =
                getCpuLimitFromCgroupV1(
                        Paths.get(CGROUP_V1_CPU_QUOTA_PATH), Paths.get(CGROUP_V1_CPU_PERIOD_PATH));
        if (limit > 0) {
            return limit;
        }

        LOG.debug(
                "Could not detect container CPU limit from cgroup files. "
                        + "This is expected when not running inside a container or when no CPU limit is set.");
        return -1;
    }

    /**
     * Reads CPU limit from cgroup v2.
     *
     * <p>The file {@code /sys/fs/cgroup/cpu.max} contains two space-separated values: {@code quota
     * period}. For example, {@code "50000 100000"} means a limit of 0.5 CPU cores. The string
     * {@code "max"} as the quota means no limit is set.
     *
     * @param cpuMaxPath path to the cgroup v2 {@code cpu.max} file
     * @return the CPU limit as a double, or {@code -1} if unavailable or unlimited
     */
    @VisibleForTesting
    static double getCpuLimitFromCgroupV2(Path cpuMaxPath) {
        try {
            if (!Files.exists(cpuMaxPath)) {
                return -1;
            }

            String content = Files.readString(cpuMaxPath).trim();
            String[] parts = content.split("\\s+");
            if (parts.length != 2) {
                LOG.debug(
                        "Unexpected format in {}: '{}'. Expected 'quota period'.",
                        cpuMaxPath,
                        content);
                return -1;
            }

            // "max" means no CPU limit is set
            if (CGROUP_V2_UNLIMITED.equals(parts[0])) {
                LOG.debug("No CPU limit set (cgroup v2 quota is '{}').", CGROUP_V2_UNLIMITED);
                return -1;
            }

            long quota = Long.parseLong(parts[0]);
            long period = Long.parseLong(parts[1]);
            if (quota > 0 && period > 0) {
                double cpuLimit = (double) quota / period;
                LOG.debug(
                        "Detected cgroup v2 CPU limit: quota={}, period={}, limit={}",
                        quota,
                        period,
                        cpuLimit);
                return cpuLimit;
            }
        } catch (NumberFormatException e) {
            LOG.debug("Failed to parse cgroup v2 CPU limit values.", e);
        } catch (IOException e) {
            LOG.debug("Could not read cgroup v2 CPU limit file: {}", cpuMaxPath, e);
        } catch (Throwable t) {
            LOG.debug("Unexpected error reading cgroup v2 CPU limit.", t);
        }
        return -1;
    }

    /**
     * Reads CPU limit from cgroup v1.
     *
     * <p>The quota is read from {@code /sys/fs/cgroup/cpu/cpu.cfs_quota_us} and the period from
     * {@code /sys/fs/cgroup/cpu/cpu.cfs_period_us}. Both values are in microseconds. A quota of
     * {@code -1} means no limit is set. The CPU limit is computed as {@code quota / period}.
     *
     * @param quotaPath path to the cgroup v1 {@code cpu.cfs_quota_us} file
     * @param periodPath path to the cgroup v1 {@code cpu.cfs_period_us} file
     * @return the CPU limit as a double, or {@code -1} if unavailable or unlimited
     */
    @VisibleForTesting
    static double getCpuLimitFromCgroupV1(Path quotaPath, Path periodPath) {
        try {
            if (!Files.exists(quotaPath) || !Files.exists(periodPath)) {
                return -1;
            }

            long quota = Long.parseLong(Files.readString(quotaPath).trim());
            long period = Long.parseLong(Files.readString(periodPath).trim());

            // quota == -1 means no CPU limit is set in cgroup v1
            if (quota <= 0) {
                LOG.debug("No CPU limit set (cgroup v1 quota={}).", quota);
                return -1;
            }

            if (period <= 0) {
                LOG.debug("Invalid cgroup v1 CPU period: {}", period);
                return -1;
            }

            double cpuLimit = (double) quota / period;
            LOG.debug(
                    "Detected cgroup v1 CPU limit: quota={}, period={}, limit={}",
                    quota,
                    period,
                    cpuLimit);
            return cpuLimit;
        } catch (NumberFormatException e) {
            LOG.debug("Failed to parse cgroup v1 CPU limit values.", e);
        } catch (IOException e) {
            LOG.debug("Could not read cgroup v1 CPU limit files.", e);
        } catch (Throwable t) {
            LOG.debug("Unexpected error reading cgroup v1 CPU limit.", t);
        }
        return -1;
    }

    /**
     * Returns the size of the physical memory in bytes.
     *
     * @return the size of the physical memory in bytes or {@code -1}, if the size could not be
     *     determined.
     */
    public static long getSizeOfPhysicalMemory() {
        // first try if the JVM can directly tell us what the system memory is
        // this works only on Oracle JVMs
        try {
            Class<?> clazz = Class.forName("com.sun.management.OperatingSystemMXBean");
            Method method = clazz.getMethod("getTotalPhysicalMemorySize");
            OperatingSystemMXBean operatingSystemMXBean =
                    ManagementFactory.getOperatingSystemMXBean();

            // someone may install different beans, so we need to check whether the bean
            // is in fact the sun management bean
            if (clazz.isInstance(operatingSystemMXBean)) {
                return (Long) method.invoke(operatingSystemMXBean);
            }
        } catch (ClassNotFoundException e) {
            // this happens on non-Oracle JVMs, do nothing and use the alternative code paths
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            LOG.warn(
                    "Access to physical memory size: "
                            + "com.sun.management.OperatingSystemMXBean incompatibly changed.",
                    e);
        }

        // we now try the OS specific access paths
        switch (OperatingSystem.getCurrentOperatingSystem()) {
            case LINUX:
                return getSizeOfPhysicalMemoryForLinux();

            case WINDOWS:
                return getSizeOfPhysicalMemoryForWindows();

            case MAC_OS:
                return getSizeOfPhysicalMemoryForMac();

            case FREE_BSD:
                return getSizeOfPhysicalMemoryForFreeBSD();

            case UNKNOWN:
                LOG.error("Cannot determine size of physical memory for unknown operating system");
                return -1;

            default:
                LOG.error("Unrecognized OS: " + OperatingSystem.getCurrentOperatingSystem());
                return -1;
        }
    }

    /**
     * Returns the size of the physical memory in bytes on a Linux-based operating system.
     *
     * @return the size of the physical memory in bytes or {@code -1}, if the size could not be
     *     determined
     */
    private static long getSizeOfPhysicalMemoryForLinux() {
        try (BufferedReader lineReader =
                new BufferedReader(new FileReader(LINUX_MEMORY_INFO_PATH))) {
            String line;
            while ((line = lineReader.readLine()) != null) {
                Matcher matcher = LINUX_MEMORY_REGEX.matcher(line);
                if (matcher.matches()) {
                    String totalMemory = matcher.group(1);
                    return Long.parseLong(totalMemory) * 1024L; // Convert from kilobyte to byte
                }
            }
            // expected line did not come
            LOG.error(
                    "Cannot determine the size of the physical memory for Linux host (using '/proc/meminfo'). "
                            + "Unexpected format.");
            return -1;
        } catch (NumberFormatException e) {
            LOG.error(
                    "Cannot determine the size of the physical memory for Linux host (using '/proc/meminfo'). "
                            + "Unexpected format.");
            return -1;
        } catch (Throwable t) {
            LOG.error(
                    "Cannot determine the size of the physical memory for Linux host (using '/proc/meminfo') ",
                    t);
            return -1;
        }
    }

    /**
     * Returns the size of the physical memory in bytes on a Mac OS-based operating system
     *
     * @return the size of the physical memory in bytes or {@code -1}, if the size could not be
     *     determined
     */
    private static long getSizeOfPhysicalMemoryForMac() {
        BufferedReader bi = null;
        try {
            Process proc = Runtime.getRuntime().exec("sysctl hw.memsize");

            bi =
                    new BufferedReader(
                            new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));

            String line;
            while ((line = bi.readLine()) != null) {
                if (line.startsWith("hw.memsize")) {
                    long memsize = Long.parseLong(line.split(":")[1].trim());
                    bi.close();
                    proc.destroy();
                    return memsize;
                }
            }

        } catch (Throwable t) {
            LOG.error("Cannot determine physical memory of machine for MacOS host", t);
            return -1;
        } finally {
            if (bi != null) {
                try {
                    bi.close();
                } catch (IOException ignored) {
                }
            }
        }
        return -1;
    }

    /**
     * Returns the size of the physical memory in bytes on FreeBSD.
     *
     * @return the size of the physical memory in bytes or {@code -1}, if the size could not be
     *     determined
     */
    private static long getSizeOfPhysicalMemoryForFreeBSD() {
        BufferedReader bi = null;
        try {
            Process proc = Runtime.getRuntime().exec("sysctl hw.physmem");

            bi =
                    new BufferedReader(
                            new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));

            String line;
            while ((line = bi.readLine()) != null) {
                if (line.startsWith("hw.physmem")) {
                    long memsize = Long.parseLong(line.split(":")[1].trim());
                    bi.close();
                    proc.destroy();
                    return memsize;
                }
            }

            LOG.error(
                    "Cannot determine the size of the physical memory for FreeBSD host "
                            + "(using 'sysctl hw.physmem').");
            return -1;
        } catch (Throwable t) {
            LOG.error(
                    "Cannot determine the size of the physical memory for FreeBSD host "
                            + "(using 'sysctl hw.physmem')",
                    t);
            return -1;
        } finally {
            if (bi != null) {
                try {
                    bi.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * Returns the size of the physical memory in bytes on Windows.
     *
     * @return the size of the physical memory in bytes or {@code -1}, if the size could not be
     *     determined
     */
    private static long getSizeOfPhysicalMemoryForWindows() {
        BufferedReader bi = null;
        try {
            Process proc = Runtime.getRuntime().exec("wmic memorychip get capacity");

            bi =
                    new BufferedReader(
                            new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));

            String line = bi.readLine();
            if (line == null) {
                return -1L;
            }

            if (!line.startsWith("Capacity")) {
                return -1L;
            }

            long sizeOfPhyiscalMemory = 0L;
            while ((line = bi.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                line = line.replaceAll(" ", "");
                sizeOfPhyiscalMemory += Long.parseLong(line);
            }
            return sizeOfPhyiscalMemory;
        } catch (Throwable t) {
            LOG.error(
                    "Cannot determine the size of the physical memory for Windows host "
                            + "(using 'wmic memorychip')",
                    t);
            return -1L;
        } finally {
            if (bi != null) {
                try {
                    bi.close();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    private Hardware() {}
}
