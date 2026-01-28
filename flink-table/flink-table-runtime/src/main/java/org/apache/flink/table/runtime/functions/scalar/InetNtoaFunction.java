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
 * Implementation of {@link BuiltInFunctionDefinitions#INET_NTOA}.
 *
 * <p>This function converts a numeric IPv4 address representation back to its string format. It
 * follows the MySQL INET_NTOA function behavior.
 *
 * <p>The conversion extracts each octet from the numeric value using bit shifting and masking.
 *
 * <p>Note: This function only supports IPv4 addresses. IPv6 addresses are not supported.
 *
 * <p><b>Implementation Note:</b> This implementation does not use utility classes such as {@code
 * com.google.common.net.InetAddresses} or {@code sun.net.util.IPAddressUtil} because:
 *
 * <ul>
 *   <li>The conversion from number to IP string is straightforward and does not benefit from
 *       external libraries
 *   <li>Using a custom implementation avoids unnecessary dependencies and object allocations (e.g.,
 *       creating {@code InetAddress} objects)
 *   <li>{@code sun.net.util.IPAddressUtil} is a JDK internal API requiring {@code --add-exports},
 *       which introduces JDK version compatibility issues
 * </ul>
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>INET_NTOA(2130706433) returns '127.0.0.1'
 *   <li>INET_NTOA(167772161) returns '10.0.0.1'
 *   <li>INET_NTOA(0) returns '0.0.0.0'
 * </ul>
 */
@Internal
public class InetNtoaFunction extends BuiltInScalarFunction {

    private static final long MAX_IPV4_VALUE = 0xFFFFFFFFL;
    private static final int OCTET_MASK = 0xFF;
    private static final int BITS_PER_OCTET = 8;

    public InetNtoaFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.INET_NTOA, context);
    }

    /**
     * Converts a numeric IPv4 address representation (Long) to its string format.
     *
     * @param ipNumber the numeric representation of the IPv4 address
     * @return the IPv4 address string in dotted-decimal notation, or null if input is null or out
     *     of valid range
     */
    public @Nullable StringData eval(@Nullable Long ipNumber) {
        if (ipNumber == null) {
            return null;
        }
        return convertToIp(ipNumber);
    }

    /**
     * Converts a numeric IPv4 address representation (Integer) to its string format.
     *
     * <p>This overload handles INT type inputs which Flink may pass directly without implicit
     * conversion to Long.
     *
     * @param ipNumber the numeric representation of the IPv4 address
     * @return the IPv4 address string in dotted-decimal notation, or null if input is null or out
     *     of valid range
     */
    public @Nullable StringData eval(@Nullable Integer ipNumber) {
        if (ipNumber == null) {
            return null;
        }
        // Convert to long, treating negative integers as unsigned
        // For example, -1 (0xFFFFFFFF as signed int) should be treated as 4294967295
        return convertToIp(Integer.toUnsignedLong(ipNumber));
    }

    /**
     * Internal conversion method.
     *
     * @param ipNumber the numeric representation
     * @return the IPv4 address string, or null if out of valid range
     */
    private @Nullable StringData convertToIp(long ipNumber) {
        // Check if the number is within valid IPv4 range [0, 4294967295]
        if (ipNumber < 0 || ipNumber > MAX_IPV4_VALUE) {
            return null;
        }

        return StringData.fromString(longToIp(ipNumber));
    }

    /**
     * Converts a numeric IPv4 address representation to its string format.
     *
     * @param ipNumber the numeric representation
     * @return the IPv4 address string
     */
    private static String longToIp(long ipNumber) {
        final int octet1 = (int) ((ipNumber >> (BITS_PER_OCTET * 3)) & OCTET_MASK);
        final int octet2 = (int) ((ipNumber >> (BITS_PER_OCTET * 2)) & OCTET_MASK);
        final int octet3 = (int) ((ipNumber >> BITS_PER_OCTET) & OCTET_MASK);
        final int octet4 = (int) (ipNumber & OCTET_MASK);

        return octet1 + "." + octet2 + "." + octet3 + "." + octet4;
    }
}
