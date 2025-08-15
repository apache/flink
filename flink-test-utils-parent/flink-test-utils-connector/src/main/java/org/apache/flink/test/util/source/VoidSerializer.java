/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.util.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * A serializer for Void checkpoint states. This is useful for test sources that don't need to store
 * any checkpoint state. The serializer always returns null during deserialization and stores no
 * data during serialization.
 */
public enum VoidSerializer implements SimpleVersionedSerializer<Void> {
    INSTANCE;

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Void obj) throws IOException {
        // Always return empty byte array for Void objects
        return new byte[0];
    }

    @Override
    public Void deserialize(int version, byte[] serialized) throws IOException {
        // Always return null - Void objects have no state to restore
        return null;
    }
}
