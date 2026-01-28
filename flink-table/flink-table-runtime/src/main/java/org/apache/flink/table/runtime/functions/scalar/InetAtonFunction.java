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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;

import javax.annotation.Nullable;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#INET_ATON}.
 *
 * <p>This function converts an IPv4 address string to its numeric representation. It follows the
 * MySQL INET_ATON function behavior, including support for short-form IPv4 addresses.
 *
 * <p>The conversion formula for a standard IP address A.B.C.D is: A * 256^3 + B * 256^2 + C * 256 +
 * D
 *
 * <p>MySQL-compatible short-form IPv4 addresses are supported:
 *
 * <ul>
 *   <li>a.b is interpreted as a.0.0.b
 *   <li>a.b.c is interpreted as a.b.0.c
 * </ul>
 *
 * <p>Leading zeros in octets are parsed as decimal (consistent with MySQL), not octal.
 *
 * <p>Note: This function only supports IPv4 addresses. IPv6 addresses are not supported.
 *
 * <p><b>Implementation Note:</b> This implementation does not use utility classes such as {@code
 * com.google.common.net.InetAddresses} or {@code sun.net.util.IPAddressUtil} because:
 *
 * <ul>
 *   <li>Guava's {@code InetAddresses.forString()} does not support MySQL-compatible short-form IP
 *       addresses (e.g., "127.1" interpreted as "127.0.0.1")
 *   <li>Standard IP parsers may interpret leading zeros as octal (e.g., "010" as 8), while MySQL
 *       treats them as decimal (e.g., "010" as 10)
 *   <li>{@code sun.net.util.IPAddressUtil} is a JDK internal API requiring {@code --add-exports},
 *       which introduces JDK version compatibility issues
 * </ul>
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>INET_ATON('127.0.0.1') returns 2130706433
 *   <li>INET_ATON('127.1') returns 2130706433 (short-form: 127.0.0.1)
 *   <li>INET_ATON('127.0.1') returns 2130706433 (short-form: 127.0.0.1)
 *   <li>INET_ATON('10.0.0.1') returns 167772161
 *   <li>INET_ATON('0.0.0.0') returns 0
 * </ul>
 */
@Internal
public class InetAtonFunction extends BuiltInScalarFunction {

    public InetAtonFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.INET_ATON, context);
    }

    /**
     * Converts an IPv4 address string to its numeric representation.
     *
     * @param ipAddress the IPv4 address string in dotted-decimal notation (supports short-form)
     * @return the numeric representation of the IP address, or null if input is null or invalid
     */
    public @Nullable Long eval(@Nullable StringData ipAddress) {
        if (ipAddress == null) {
            return null;
        }

        final String ip = ipAddress.toString();
        if (ip.isEmpty()) {
            return null;
        }

        return ipToLong(ip);
    }

    /**
     * Converts an IPv4 address string to a long value.
     *
     * <p>Supports MySQL-compatible short-form addresses:
     *
     * <ul>
     *   <li>a.b -> a.0.0.b
     *   <li>a.b.c -> a.b.0.c
     *   <li>a.b.c.d -> standard format
     * </ul>
     *
     * <p>Leading zeros are treated as decimal (not octal), consistent with MySQL behavior.
     *
     * @param ip the IPv4 address string
     * @return the long value, or null if the IP address is invalid
     */
    private static @Nullable Long ipToLong(String ip) {
        int len = ip.length();
        int octetCount = 0;
        int octetStart = 0;
        long[] octets = new long[4];

        for (int i = 0; i <= len; i++) {
            if (i == len || ip.charAt(i) == '.') {
                if (octetCount >= 4) {
                    return null;
                }
                if (octetStart == i) {
                    return null;
                }

                // Parse number manually
                long octet = 0;
                for (int j = octetStart; j < i; j++) {
                    char c = ip.charAt(j);
                    if (c < '0' || c > '9') {
                        return null;
                    }
                    octet = octet * 10 + (c - '0');
                    if (octet > 255) {
                        return null;
                    }
                }
                octets[octetCount++] = octet;
                octetStart = i + 1;
            }
        }

        if (octetCount < 2) {
            return null;
        }

        switch (octetCount) {
            case 2:
                return (octets[0] << 24) | octets[1];
            case 3:
                return (octets[0] << 24) | (octets[1] << 16) | octets[2];
            case 4:
                return (octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3];
            default:
                return null;
        }
    }
}
