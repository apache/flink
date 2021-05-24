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

package org.apache.flink.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The processor architecture of the this system.
 *
 * <p>Note that the memory address size might be different than the actual hardware architecture,
 * due to the installed OS (32bit OS) or when installing a 32 bit JRE in a 64 bit OS.
 */
public enum ProcessorArchitecture {

    /** The Intel x86 processor architecture. */
    X86(MemoryAddressSize._32_BIT, "x86", "i386", "i486", "i586", "i686"),

    /** The AMD 64 bit processor architecture. */
    AMD64(MemoryAddressSize._64_BIT, "amd64", "x86_64"),

    /** The ARM 32 bit processor architecture. */
    ARMv7(MemoryAddressSize._32_BIT, "armv7", "arm"),

    /** The 64 bit ARM processor architecture. */
    AARCH64(MemoryAddressSize._64_BIT, "aarch64"),

    /** The little-endian mode of the 64 bit Power-PC architecture. */
    PPC64_LE(MemoryAddressSize._64_BIT, "ppc64le"),

    /**
     * Unknown architecture, could not be determined. This one conservatively assumes 32 bit,
     * because 64 bit platforms typically support 32 bit memory spaces.
     */
    UNKNOWN(MemoryAddressSize._32_BIT, "unknown");

    // ------------------------------------------------------------------------

    private static final ProcessorArchitecture CURRENT = readArchFromSystemProperties();

    private final MemoryAddressSize addressSize;

    private final String name;

    private final List<String> alternativeNames;

    ProcessorArchitecture(MemoryAddressSize addressSize, String name, String... alternativeNames) {
        this.addressSize = addressSize;
        this.name = name;
        this.alternativeNames = Collections.unmodifiableList(Arrays.asList(alternativeNames));
    }

    /** Gets the address size of the memory (32 bit, 64 bit). */
    public MemoryAddressSize getAddressSize() {
        return addressSize;
    }

    /**
     * Gets the primary name of the processor architecture. The primary name would for example be
     * "x86" or "amd64".
     */
    public String getArchitectureName() {
        return name;
    }

    /**
     * Gets the alternative names for the processor architecture. Alternative names are for example
     * "i586" for "x86", or "x86_64" for "amd64".
     */
    public List<String> getAlternativeNames() {
        return alternativeNames;
    }

    // ------------------------------------------------------------------------

    /** Gets the ProcessorArchitecture of the system running this process. */
    public static ProcessorArchitecture getProcessorArchitecture() {
        return CURRENT;
    }

    /**
     * Gets the MemorySize of the ProcessorArchitecture of this process.
     *
     * <p>Note that the memory address size might be different than the actual hardware
     * architecture, due to the installed OS (32bit OS) or when installing a 32 bit JRE in a 64 bit
     * OS.
     */
    public static MemoryAddressSize getMemoryAddressSize() {
        return getProcessorArchitecture().getAddressSize();
    }

    private static ProcessorArchitecture readArchFromSystemProperties() {
        final String sysArchName = System.getProperty("os.arch");
        if (sysArchName == null) {
            return UNKNOWN;
        }

        for (ProcessorArchitecture arch : values()) {
            if (sysArchName.equalsIgnoreCase(arch.name)) {
                return arch;
            }

            for (String altName : arch.alternativeNames) {
                if (sysArchName.equalsIgnoreCase(altName)) {
                    return arch;
                }
            }
        }

        return UNKNOWN;
    }

    // ------------------------------------------------------------------------

    /** The memory address size of the processor. */
    public enum MemoryAddressSize {

        /** 32 bit memory address size. */
        _32_BIT,

        /** 64 bit memory address size. */
        _64_BIT,
    }
}
