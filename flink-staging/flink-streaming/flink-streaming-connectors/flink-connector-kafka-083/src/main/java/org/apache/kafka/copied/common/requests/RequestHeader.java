/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.copied.common.requests;

import org.apache.kafka.copied.common.protocol.ProtoUtils;
import org.apache.kafka.copied.common.protocol.Protocol;
import org.apache.kafka.copied.common.protocol.types.Field;
import org.apache.kafka.copied.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/**
 * The header for a request in the Kafka protocol
 */
public class RequestHeader extends AbstractRequestResponse {

    private static final Field API_KEY_FIELD = Protocol.REQUEST_HEADER.get("api_key");
    private static final Field API_VERSION_FIELD = Protocol.REQUEST_HEADER.get("api_version");
    private static final Field CLIENT_ID_FIELD = Protocol.REQUEST_HEADER.get("client_id");
    private static final Field CORRELATION_ID_FIELD = Protocol.REQUEST_HEADER.get("correlation_id");

    private final short apiKey;
    private final short apiVersion;
    private final String clientId;
    private final int correlationId;

    public RequestHeader(Struct header) {
        super(header);
        apiKey = struct.getShort(API_KEY_FIELD);
        apiVersion = struct.getShort(API_VERSION_FIELD);
        clientId = struct.getString(CLIENT_ID_FIELD);
        correlationId = struct.getInt(CORRELATION_ID_FIELD);
    }

    public RequestHeader(short apiKey, String client, int correlation) {
        this(apiKey, ProtoUtils.latestVersion(apiKey), client, correlation);
    }

    public RequestHeader(short apiKey, short version, String client, int correlation) {
        super(new Struct(Protocol.REQUEST_HEADER));
        struct.set(API_KEY_FIELD, apiKey);
        struct.set(API_VERSION_FIELD, version);
        struct.set(CLIENT_ID_FIELD, client);
        struct.set(CORRELATION_ID_FIELD, correlation);
        this.apiKey = apiKey;
        this.apiVersion = version;
        this.clientId = client;
        this.correlationId = correlation;
    }

    public short apiKey() {
        return apiKey;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public String clientId() {
        return clientId;
    }

    public int correlationId() {
        return correlationId;
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        return new RequestHeader((Struct) Protocol.REQUEST_HEADER.read(buffer));
    }
}
