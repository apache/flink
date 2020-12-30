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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Convenience class to extract hardware specifics of the computer executing the running JVM. */
public class Hardware {

    private static final Logger LOG = LoggerFactory.getLogger(Hardware.class);

    private static final String LINUX_MEMORY_INFO_PATH = "/proc/meminfo";

    private static final Pattern LINUX_MEMORY_REGEX =
            Pattern.compile("^MemTotal:\\s*(\\d+)\\s+kB$");

    // ------------------------------------------------------------------------

    /**
     * Gets the number of CPU cores (hardware contexts) that the JVM has access to.
     *
     * @return The number of CPU cores.
     */
    public static int getNumberCPUCores() {
        return Runtime.getRuntime().availableProcessors();
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

            bi = new BufferedReader(new InputStreamReader(proc.getInputStream()));

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

            bi = new BufferedReader(new InputStreamReader(proc.getInputStream()));

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

            bi = new BufferedReader(new InputStreamReader(proc.getInputStream()));

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
