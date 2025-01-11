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

package org.apache.flink.streaming.api.graph.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nullable;

/** Helper class carries the data required to updates a stream edge. */
@Internal
public class StreamNodeUpdateRequestInfo {
    private final Integer nodeId;

    // Null means it does not request to change the typeSerializersIn.
    @Nullable private TypeSerializer<?>[] typeSerializersIn;

    public StreamNodeUpdateRequestInfo(Integer nodeId) {
        this.nodeId = nodeId;
    }

    public StreamNodeUpdateRequestInfo withTypeSerializersIn(
            TypeSerializer<?>[] typeSerializersIn) {
        this.typeSerializersIn = typeSerializersIn;
        return this;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    @Nullable
    public TypeSerializer<?>[] getTypeSerializersIn() {
        return typeSerializersIn;
    }
}
