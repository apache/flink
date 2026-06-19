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

package org.apache.flink.runtime.security.token.hadoop;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/** Delegation token serializer and deserializer functionality. */
@Internal
public class HadoopDelegationTokenConverter {
    /** Serializes delegation tokens. */
    public static byte[] serialize(Credentials credentials) throws IOException {
        try (DataOutputBuffer dob = new DataOutputBuffer()) {
            credentials.writeTokenStorageToStream(dob);
            return dob.getData();
        }
    }

    /** Deserializes delegation tokens. */
    public static Credentials deserialize(byte[] credentialsBytes) throws IOException {
        try (DataInputStream dis =
                new DataInputStream(new ByteArrayInputStream(credentialsBytes))) {
            Credentials credentials = new Credentials();
            credentials.readTokenStorageStream(dis);
            return credentials;
        }
    }
}
