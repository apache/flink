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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.StreamStateHandle;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * V7 serializer. Identical to V6 except that operator coordinator state may be stored in its own
 * file, which the metadata references, rather than always inline.
 *
 * <p>This version is only written when some coordinator state is in its own file, so checkpoints
 * whose coordinator state all stays inline keep the V6 format and stay readable by Flink 2.1 and
 * later.
 */
@Internal
public class MetadataV7Serializer extends MetadataV6Serializer {

    public static final MetadataSerializer INSTANCE = new MetadataV7Serializer();

    public static final int VERSION = 7;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    protected void serializeCoordinatorState(
            @Nullable StreamStateHandle coordinatorState,
            DataOutputStream dos,
            SerializationContext context)
            throws IOException {
        serializeStreamStateHandle(coordinatorState, dos, context);
    }

    @Nullable
    @Override
    protected StreamStateHandle deserializeCoordinatorState(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
        return deserializeStreamStateHandle(dis, context);
    }
}
