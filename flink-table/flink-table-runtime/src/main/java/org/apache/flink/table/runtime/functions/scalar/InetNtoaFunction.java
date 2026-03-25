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
 * <p>This function converts a numeric IPv4 address representation back to its string format.
 *
 * <p>The conversion extracts each octet from the numeric value using bit shifting and masking.
 *
 * <p>Accepts any integer numeric type (TINYINT, SMALLINT, INT, BIGINT). Negative values return
 * null, consistent with MySQL's {@code INET_NTOA(-1) = NULL} behavior.
 *
 * <p>Note: This function only supports IPv4 addresses. IPv6 addresses are not supported.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>INET_NTOA(2130706433) returns '127.0.0.1'
 *   <li>INET_NTOA(167772161) returns '10.0.0.1'
 *   <li>INET_NTOA(0) returns '0.0.0.0'
 *   <li>INET_NTOA(-1) returns NULL
 * </ul>
 */
@Internal
public class InetNtoaFunction extends BuiltInScalarFunction {

    public InetNtoaFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.INET_NTOA, context);
    }

    /**
     * Converts a numeric IPv4 address representation to its string format.
     *
     * <p>Accepts any integer numeric type (TINYINT, SMALLINT, INT, BIGINT). Negative values return
     * null, consistent with MySQL's {@code INET_NTOA(-1) = NULL} behavior.
     *
     * @param ipNumber the numeric representation of the IPv4 address
     * @return the IPv4 address string in dotted-decimal notation, or null if input is null,
     *     negative, or out of valid range [0, 4294967295]
     */
    public @Nullable StringData eval(@Nullable Number ipNumber) {
        if (ipNumber == null) {
            return null;
        }
        return longToIp(ipNumber.longValue());
    }

    /**
     * Converts a numeric IPv4 address to its dotted-decimal string format.
     *
     * @param ipNumber the numeric representation
     * @return the IPv4 address string, or null if out of valid range [0, 4294967295]
     */
    private static @Nullable StringData longToIp(long ipNumber) {
        if (ipNumber < 0 || ipNumber > 0xFFFFFFFFL) {
            return null;
        }

        return StringData.fromString(
                ((ipNumber >> 24) & 0xFF)
                        + "."
                        + ((ipNumber >> 16) & 0xFF)
                        + "."
                        + ((ipNumber >> 8) & 0xFF)
                        + "."
                        + (ipNumber & 0xFF));
    }
}
