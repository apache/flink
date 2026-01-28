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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Tests for {@link BuiltInFunctionDefinitions#INET_ATON} and {@link
 * BuiltInFunctionDefinitions#INET_NTOA}.
 */
public class InetFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                inetAtonStandardTestCases(),
                inetAtonShortFormTestCases(),
                inetAtonLeadingZeroTestCases(),
                inetAtonInvalidInputTestCases(),
                inetNtoaTestCases(),
                inetNtoaIntInputTestCases());
    }

    /** Test standard 4-octet IPv4 addresses. */
    private TestSetSpec inetAtonStandardTestCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.INET_ATON)
                .onFieldsWithData(
                        "127.0.0.1", // f0: loopback -> 2130706433
                        "0.0.0.0", // f1: zero -> 0
                        "255.255.255.255", // f2: max -> 4294967295
                        "192.168.1.1", // f3: -> 3232235777
                        "10.0.0.1" // f4: -> 167772161
                        )
                .andDataTypes(STRING(), STRING(), STRING(), STRING(), STRING())
                .testResult($("f0").inetAton(), "INET_ATON(f0)", 2130706433L, BIGINT())
                .testResult($("f1").inetAton(), "INET_ATON(f1)", 0L, BIGINT())
                .testResult($("f2").inetAton(), "INET_ATON(f2)", 4294967295L, BIGINT())
                .testResult($("f3").inetAton(), "INET_ATON(f3)", 3232235777L, BIGINT())
                .testResult($("f4").inetAton(), "INET_ATON(f4)", 167772161L, BIGINT());
    }

    /** Test MySQL-compatible short-form IPv4 addresses: a.b -> a.0.0.b, a.b.c -> a.b.0.c. */
    private TestSetSpec inetAtonShortFormTestCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.INET_ATON)
                .onFieldsWithData(
                        "127.1", // f0: short form a.b -> 127.0.0.1 = 2130706433
                        "127.0.1", // f1: short form a.b.c -> 127.0.0.1 = 2130706433
                        "10.1", // f2: short form a.b -> 10.0.0.1 = 167772161
                        "192.168.1", // f3: short form a.b.c -> 192.168.0.1 = 3232235521
                        "1.2" // f4: short form a.b -> 1.0.0.2 = 16777218
                        )
                .andDataTypes(STRING(), STRING(), STRING(), STRING(), STRING())
                .testResult($("f0").inetAton(), "INET_ATON(f0)", 2130706433L, BIGINT())
                .testResult($("f1").inetAton(), "INET_ATON(f1)", 2130706433L, BIGINT())
                .testResult($("f2").inetAton(), "INET_ATON(f2)", 167772161L, BIGINT())
                .testResult($("f3").inetAton(), "INET_ATON(f3)", 3232235521L, BIGINT())
                .testResult($("f4").inetAton(), "INET_ATON(f4)", 16777218L, BIGINT());
    }

    /** Test leading zeros are parsed as decimal (MySQL behavior), not octal. */
    private TestSetSpec inetAtonLeadingZeroTestCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.INET_ATON)
                .onFieldsWithData(
                        "010.000.000.001", // f0: leading zeros as decimal (10.0.0.1) = 167772161
                        "192.168.001.001", // f1: leading zeros as decimal (192.168.1.1) =
                        // 3232235777
                        "001.002.003.004" // f2: leading zeros as decimal (1.2.3.4) = 16909060
                        )
                .andDataTypes(STRING(), STRING(), STRING())
                .testResult($("f0").inetAton(), "INET_ATON(f0)", 167772161L, BIGINT())
                .testResult($("f1").inetAton(), "INET_ATON(f1)", 3232235777L, BIGINT())
                .testResult($("f2").inetAton(), "INET_ATON(f2)", 16909060L, BIGINT());
    }

    /** Test invalid inputs return null. */
    private TestSetSpec inetAtonInvalidInputTestCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.INET_ATON)
                .onFieldsWithData(
                        null, // f0: null
                        "", // f1: empty
                        "invalid", // f2: invalid format
                        "256.0.0.1", // f3: octet out of range (256 > 255)
                        "1.2.3.4.5", // f4: extra octet (5 octets)
                        "1.2.3.", // f5: trailing dot
                        ".1.2.3", // f6: leading dot
                        "1..2.3", // f7: double dot
                        "1", // f8: single octet (invalid)
                        " 127.0.0.1", // f9: leading space (no trim)
                        "127.0.0.1 " // f10: trailing space (no trim)
                        )
                .andDataTypes(
                        STRING(), STRING(), STRING(), STRING(), STRING(), STRING(), STRING(),
                        STRING(), STRING(), STRING(), STRING())
                .testResult($("f0").inetAton(), "INET_ATON(f0)", null, BIGINT().nullable())
                .testResult($("f1").inetAton(), "INET_ATON(f1)", null, BIGINT().nullable())
                .testResult($("f2").inetAton(), "INET_ATON(f2)", null, BIGINT().nullable())
                .testResult($("f3").inetAton(), "INET_ATON(f3)", null, BIGINT().nullable())
                .testResult($("f4").inetAton(), "INET_ATON(f4)", null, BIGINT().nullable())
                .testResult($("f5").inetAton(), "INET_ATON(f5)", null, BIGINT().nullable())
                .testResult($("f6").inetAton(), "INET_ATON(f6)", null, BIGINT().nullable())
                .testResult($("f7").inetAton(), "INET_ATON(f7)", null, BIGINT().nullable())
                .testResult($("f8").inetAton(), "INET_ATON(f8)", null, BIGINT().nullable())
                .testResult($("f9").inetAton(), "INET_ATON(f9)", null, BIGINT().nullable())
                .testResult($("f10").inetAton(), "INET_ATON(f10)", null, BIGINT().nullable());
    }

    /** Test INET_NTOA with BIGINT inputs. */
    private TestSetSpec inetNtoaTestCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.INET_NTOA)
                .onFieldsWithData(
                        2130706433L, // f0: 127.0.0.1
                        0L, // f1: 0.0.0.0
                        4294967295L, // f2: 255.255.255.255
                        3232235777L, // f3: 192.168.1.1
                        167772161L, // f4: 10.0.0.1
                        null, // f5: null
                        -1L, // f6: negative (invalid)
                        4294967296L // f7: exceeds max (invalid)
                        )
                .andDataTypes(
                        BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(),
                        BIGINT())
                .testResult($("f0").inetNtoa(), "INET_NTOA(f0)", "127.0.0.1", STRING())
                .testResult($("f1").inetNtoa(), "INET_NTOA(f1)", "0.0.0.0", STRING())
                .testResult($("f2").inetNtoa(), "INET_NTOA(f2)", "255.255.255.255", STRING())
                .testResult($("f3").inetNtoa(), "INET_NTOA(f3)", "192.168.1.1", STRING())
                .testResult($("f4").inetNtoa(), "INET_NTOA(f4)", "10.0.0.1", STRING())
                .testResult($("f5").inetNtoa(), "INET_NTOA(f5)", null, STRING().nullable())
                .testResult($("f6").inetNtoa(), "INET_NTOA(f6)", null, STRING().nullable())
                .testResult($("f7").inetNtoa(), "INET_NTOA(f7)", null, STRING().nullable());
    }

    /** Test INET_NTOA with INT inputs. */
    private TestSetSpec inetNtoaIntInputTestCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.INET_NTOA)
                .onFieldsWithData(
                        2130706433, // f0: 127.0.0.1 (as INT)
                        0, // f1: 0.0.0.0 (as INT)
                        167772161, // f2: 10.0.0.1 (as INT)
                        (Integer) null, // f3: null (as INT)
                        -1 // f4: -1 as unsigned int -> 4294967295 -> 255.255.255.255
                        )
                .andDataTypes(INT(), INT(), INT(), INT(), INT())
                .testResult($("f0").inetNtoa(), "INET_NTOA(f0)", "127.0.0.1", STRING())
                .testResult($("f1").inetNtoa(), "INET_NTOA(f1)", "0.0.0.0", STRING())
                .testResult($("f2").inetNtoa(), "INET_NTOA(f2)", "10.0.0.1", STRING())
                .testResult($("f3").inetNtoa(), "INET_NTOA(f3)", null, STRING().nullable())
                .testResult($("f4").inetNtoa(), "INET_NTOA(f4)", "255.255.255.255", STRING());
    }
}
