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
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;

import javax.annotation.Nullable;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#INET_ATON}.
 *
 * <p>This function converts an IPv4 address string to its numeric representation. It is partially
 * compatible with MySQL INET_ATON, including support for short-form IPv4 addresses.
 *
 * <p>The conversion formula for a standard IP address A.B.C.D is: A * 256^3 + B * 256^2 + C * 256 +
 * D
 *
 * <p>MySQL-compatible short-form IPv4 addresses are supported:
 *
 * <ul>
 *   <li>a — the value is stored directly as an address (value must be in [0, 255])
 *   <li>a.b — interpreted as a.0.0.b
 *   <li>a.b.c — interpreted as a.b.0.c
 *   <li>a.b.c.d — standard dotted-decimal format
 * </ul>
 *
 * <p>Leading zeros in octets are parsed as decimal (consistent with MySQL), not octal.
 *
 * <p>Note: This function only supports IPv4 addresses. IPv6 addresses are not supported.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>INET_ATON('1') returns 1 (single number)
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

        BinaryStringData binaryStr = (BinaryStringData) ipAddress;
        if (BinaryStringDataUtil.isEmpty(binaryStr)) {
            return null;
        }

        return ipToLong(binaryStr);
    }

    /**
     * Converts an IPv4 address string to a long value.
     *
     * <p>Operates directly on UTF-8 bytes from {@link BinaryStringData} to avoid unnecessary String
     * object allocation. Since IPv4 addresses contain only ASCII characters ('0'-'9' and '.'), each
     * character is exactly one byte in UTF-8 encoding.
     *
     * <p>Supports MySQL-compatible short-form addresses:
     *
     * <ul>
     *   <li>a — direct value (value must be in [0, 255])
     *   <li>a.b — a.0.0.b (each part must be in [0, 255])
     *   <li>a.b.c — a.b.0.c (each part must be in [0, 255])
     *   <li>a.b.c.d — standard format (each part must be in [0, 255])
     * </ul>
     *
     * <p>Leading zeros are treated as decimal (not octal), consistent with MySQL behavior.
     *
     * @param ip the IPv4 address as BinaryStringData
     * @return the long value, or null if the IP address is invalid
     */
    private static @Nullable Long ipToLong(BinaryStringData ip) {
        int len = ip.getSizeInBytes();
        int partCount = 0;
        int partStart = 0;
        long[] parts = new long[4];

        for (int i = 0; i <= len; i++) {
            if (i == len || ip.byteAt(i) == '.') {
                if (partCount >= 4) {
                    return null;
                }
                if (partStart == i) {
                    return null;
                }

                // Parse number manually from bytes
                long value = 0;
                for (int j = partStart; j < i; j++) {
                    byte b = ip.byteAt(j);
                    if (b < '0' || b > '9') {
                        return null;
                    }
                    value = value * 10 + (b - '0');
                    if (value > 255) {
                        return null;
                    }
                }
                parts[partCount++] = value;
                partStart = i + 1;
            }
        }

        switch (partCount) {
            case 1:
                // Single number: direct value
                return parts[0];
            case 2:
                // a.b -> a.0.0.b
                return (parts[0] << 24) | parts[1];
            case 3:
                // a.b.c -> a.b.0.c
                return (parts[0] << 24) | (parts[1] << 16) | parts[2];
            case 4:
                // a.b.c.d -> standard format
                return (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3];
            default:
                return null;
        }
    }
}
