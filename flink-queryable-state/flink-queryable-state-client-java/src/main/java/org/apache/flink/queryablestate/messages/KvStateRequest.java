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

package org.apache.flink.queryablestate.messages;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The request to be sent by the {@link org.apache.flink.queryablestate.client.QueryableStateClient
 * Queryable State Client} to the Client Proxy requesting a given state.
 */
@Internal
public class KvStateRequest extends MessageBody {

    private final JobID jobId;
    private final String stateName;
    private final int keyHashCode;
    private final byte[] serializedKeyAndNamespace;

    public KvStateRequest(
            final JobID jobId,
            final String stateName,
            final int keyHashCode,
            final byte[] serializedKeyAndNamespace) {

        this.jobId = Preconditions.checkNotNull(jobId);
        this.stateName = Preconditions.checkNotNull(stateName);
        this.keyHashCode = keyHashCode;
        this.serializedKeyAndNamespace = Preconditions.checkNotNull(serializedKeyAndNamespace);
    }

    public JobID getJobId() {
        return jobId;
    }

    public String getStateName() {
        return stateName;
    }

    public int getKeyHashCode() {
        return keyHashCode;
    }

    public byte[] getSerializedKeyAndNamespace() {
        return serializedKeyAndNamespace;
    }

    @Override
    public byte[] serialize() {

        byte[] serializedStateName = stateName.getBytes(ConfigConstants.DEFAULT_CHARSET);

        // JobID + stateName + sizeOf(stateName) + hashCode + keyAndNamespace +
        // sizeOf(keyAndNamespace)
        final int size =
                JobID.SIZE
                        + serializedStateName.length
                        + Integer.BYTES
                        + Integer.BYTES
                        + serializedKeyAndNamespace.length
                        + Integer.BYTES;

        return ByteBuffer.allocate(size)
                .putLong(jobId.getLowerPart())
                .putLong(jobId.getUpperPart())
                .putInt(serializedStateName.length)
                .put(serializedStateName)
                .putInt(keyHashCode)
                .putInt(serializedKeyAndNamespace.length)
                .put(serializedKeyAndNamespace)
                .array();
    }

    @Override
    public String toString() {
        return "KvStateRequest{"
                + "jobId="
                + jobId
                + ", stateName='"
                + stateName
                + '\''
                + ", keyHashCode="
                + keyHashCode
                + ", serializedKeyAndNamespace="
                + Arrays.toString(serializedKeyAndNamespace)
                + '}';
    }

    /** A {@link MessageDeserializer deserializer} for {@link KvStateRequest}. */
    public static class KvStateRequestDeserializer implements MessageDeserializer<KvStateRequest> {

        @Override
        public KvStateRequest deserializeMessage(ByteBuf buf) {
            JobID jobId = new JobID(buf.readLong(), buf.readLong());

            int statenameLength = buf.readInt();
            Preconditions.checkArgument(
                    statenameLength >= 0,
                    "Negative length for state name. " + "This indicates a serialization error.");

            String stateName = "";
            if (statenameLength > 0) {
                byte[] name = new byte[statenameLength];
                buf.readBytes(name);
                stateName = new String(name, ConfigConstants.DEFAULT_CHARSET);
            }

            int keyHashCode = buf.readInt();

            int knamespaceLength = buf.readInt();
            Preconditions.checkArgument(
                    knamespaceLength >= 0,
                    "Negative length for key and namespace. "
                            + "This indicates a serialization error.");

            byte[] serializedKeyAndNamespace = new byte[knamespaceLength];
            if (knamespaceLength > 0) {
                buf.readBytes(serializedKeyAndNamespace);
            }
            return new KvStateRequest(jobId, stateName, keyHashCode, serializedKeyAndNamespace);
        }
    }
}
