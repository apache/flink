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

package org.apache.flink.table.endpoint.hive.util;

import org.apache.flink.table.gateway.api.session.SessionHandle;

import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Conversion between thrift object and flink object. */
public class ThriftObjectConversions {

    private static final UUID SECRET_ID = UUID.fromString("b06fa16a-3d16-475f-b510-6c64abb9b173");

    // --------------------------------------------------------------------------------------------
    // Flink SessionHandle from/to Hive SessionHandle
    // --------------------------------------------------------------------------------------------

    public static TSessionHandle toTSessionHandle(SessionHandle sessionHandle) {
        return new TSessionHandle(toTHandleIdentifier(sessionHandle.getIdentifier()));
    }

    public static SessionHandle toSessionHandle(TSessionHandle tSessionHandle) {
        ByteBuffer bb = ByteBuffer.wrap(tSessionHandle.getSessionId().getGuid());
        return new SessionHandle(new UUID(bb.getLong(), bb.getLong()));
    }

    public static TStatus toTStatus(Throwable t) {
        TStatus tStatus = new TStatus(TStatusCode.ERROR_STATUS);
        tStatus.setErrorMessage(t.getMessage());
        tStatus.setInfoMessages(toString(t));
        return tStatus;
    }

    // --------------------------------------------------------------------------------------------

    private static THandleIdentifier toTHandleIdentifier(UUID publicId) {
        byte[] guid = new byte[16];
        byte[] secret = new byte[16];
        ByteBuffer guidBB = ByteBuffer.wrap(guid);
        ByteBuffer secretBB = ByteBuffer.wrap(secret);

        guidBB.putLong(publicId.getMostSignificantBits());
        guidBB.putLong(publicId.getLeastSignificantBits());
        secretBB.putLong(SECRET_ID.getMostSignificantBits());
        secretBB.putLong(SECRET_ID.getLeastSignificantBits());
        return new THandleIdentifier(ByteBuffer.wrap(guid), ByteBuffer.wrap(secret));
    }

    /**
     * Converts a {@link Throwable} object into a flattened list of texts including its stack trace
     * and the stack traces of the nested causes.
     *
     * @param ex a {@link Throwable} object
     * @return a flattened list of texts including the {@link Throwable} object's stack trace and
     *     the stack traces of the nested causes.
     */
    private static List<String> toString(Throwable ex) {
        return toString(ex, null);
    }

    private static List<String> toString(Throwable cause, StackTraceElement[] parent) {
        StackTraceElement[] trace = cause.getStackTrace();
        int m = trace.length - 1;
        if (parent != null) {
            int n = parent.length - 1;
            while (m >= 0 && n >= 0 && trace[m].equals(parent[n])) {
                m--;
                n--;
            }
        }
        List<String> detail = enroll(cause, trace, m);
        cause = cause.getCause();
        if (cause != null) {
            detail.addAll(toString(cause, trace));
        }
        return detail;
    }

    private static List<String> enroll(Throwable ex, StackTraceElement[] trace, int max) {
        List<String> details = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        builder.append('*').append(ex.getClass().getName()).append(':');
        builder.append(ex.getMessage()).append(':');
        builder.append(trace.length).append(':').append(max);
        details.add(builder.toString());
        for (int i = 0; i <= max; i++) {
            builder.setLength(0);
            builder.append(trace[i].getClassName()).append(':');
            builder.append(trace[i].getMethodName()).append(':');
            String fileName = trace[i].getFileName();
            builder.append(fileName == null ? "" : fileName).append(':');
            builder.append(trace[i].getLineNumber());
            details.add(builder.toString());
        }
        return details;
    }
}
