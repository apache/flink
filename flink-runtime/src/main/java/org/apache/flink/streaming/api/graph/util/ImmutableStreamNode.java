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
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Helper class that provides read-only StreamNode. */
@Internal
public class ImmutableStreamNode {
    private final StreamNode streamNode;
    private List<ImmutableStreamEdge> immutableOutEdges = null;
    private List<ImmutableStreamEdge> immutableInEdges = null;

    public ImmutableStreamNode(StreamNode streamNode) {
        this.streamNode = streamNode;
    }

    public List<ImmutableStreamEdge> getOutEdges() {
        if (immutableOutEdges == null) {
            immutableOutEdges = new ArrayList<>();
            for (StreamEdge edge : streamNode.getOutEdges()) {
                immutableOutEdges.add(new ImmutableStreamEdge(edge));
            }
        }
        return Collections.unmodifiableList(immutableOutEdges);
    }

    public List<ImmutableStreamEdge> getInEdges() {
        if (immutableInEdges == null) {
            immutableInEdges = new ArrayList<>();
            for (StreamEdge edge : streamNode.getInEdges()) {
                immutableInEdges.add(new ImmutableStreamEdge(edge));
            }
        }
        return Collections.unmodifiableList(immutableInEdges);
    }

    public int getId() {
        return streamNode.getId();
    }

    public @Nullable StreamOperatorFactory<?> getOperatorFactory() {
        return streamNode.getOperatorFactory();
    }

    public int getMaxParallelism() {
        return streamNode.getMaxParallelism();
    }

    public int getParallelism() {
        return streamNode.getParallelism();
    }

    public TypeSerializer<?>[] getTypeSerializersIn() {
        return Arrays.stream(streamNode.getTypeSerializersIn())
                .filter(Objects::nonNull)
                .toArray(TypeSerializer<?>[]::new);
    }

    @Override
    public String toString() {
        return streamNode.toString();
    }
}
