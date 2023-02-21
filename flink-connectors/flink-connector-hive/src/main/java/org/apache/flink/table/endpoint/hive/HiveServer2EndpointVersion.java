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

package org.apache.flink.table.endpoint.hive;

import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;

import org.apache.hive.service.rpc.thrift.TProtocolVersion;

/** Mapping between {@link HiveServer2EndpointVersion} and {@link TProtocolVersion} in Hive. */
public enum HiveServer2EndpointVersion implements EndpointVersion {
    HIVE_CLI_SERVICE_PROTOCOL_V1(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1),

    HIVE_CLI_SERVICE_PROTOCOL_V2(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2),

    HIVE_CLI_SERVICE_PROTOCOL_V3(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3),

    HIVE_CLI_SERVICE_PROTOCOL_V4(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V4),

    HIVE_CLI_SERVICE_PROTOCOL_V5(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5),

    HIVE_CLI_SERVICE_PROTOCOL_V6(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6),

    HIVE_CLI_SERVICE_PROTOCOL_V7(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7),

    HIVE_CLI_SERVICE_PROTOCOL_V8(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),

    HIVE_CLI_SERVICE_PROTOCOL_V9(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9),

    HIVE_CLI_SERVICE_PROTOCOL_V10(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);

    private final TProtocolVersion version;

    HiveServer2EndpointVersion(TProtocolVersion version) {
        this.version = version;
    }

    public TProtocolVersion getVersion() {
        return version;
    }

    public static HiveServer2EndpointVersion valueOf(TProtocolVersion version) {
        switch (version) {
            case HIVE_CLI_SERVICE_PROTOCOL_V1:
                return HIVE_CLI_SERVICE_PROTOCOL_V1;
            case HIVE_CLI_SERVICE_PROTOCOL_V2:
                return HIVE_CLI_SERVICE_PROTOCOL_V2;
            case HIVE_CLI_SERVICE_PROTOCOL_V3:
                return HIVE_CLI_SERVICE_PROTOCOL_V3;
            case HIVE_CLI_SERVICE_PROTOCOL_V4:
                return HIVE_CLI_SERVICE_PROTOCOL_V4;
            case HIVE_CLI_SERVICE_PROTOCOL_V5:
                return HIVE_CLI_SERVICE_PROTOCOL_V5;
            case HIVE_CLI_SERVICE_PROTOCOL_V6:
                return HIVE_CLI_SERVICE_PROTOCOL_V6;
            case HIVE_CLI_SERVICE_PROTOCOL_V7:
                return HIVE_CLI_SERVICE_PROTOCOL_V7;
            case HIVE_CLI_SERVICE_PROTOCOL_V8:
                return HIVE_CLI_SERVICE_PROTOCOL_V8;
            case HIVE_CLI_SERVICE_PROTOCOL_V9:
                return HIVE_CLI_SERVICE_PROTOCOL_V9;
            case HIVE_CLI_SERVICE_PROTOCOL_V10:
                return HIVE_CLI_SERVICE_PROTOCOL_V10;
            default:
                throw new IllegalArgumentException(
                        String.format("Unknown TProtocolVersion: %s.", version));
        }
    }
}
