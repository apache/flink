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

package org.apache.flink.cep.nfa.sharedbuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** An entry in {@link SharedBuffer} that allows to store relations between different entries. */
public class SharedBufferNode {

    private final List<Lockable<SharedBufferEdge>> edges;

    public SharedBufferNode() {
        edges = new ArrayList<>();
    }

    SharedBufferNode(List<Lockable<SharedBufferEdge>> edges) {
        this.edges = edges;
    }

    public List<Lockable<SharedBufferEdge>> getEdges() {
        return edges;
    }

    public void addEdge(SharedBufferEdge edge) {
        edges.add(new Lockable<>(edge, 0));
    }

    @Override
    public String toString() {
        return "SharedBufferNode{" + "edges=" + edges + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SharedBufferNode that = (SharedBufferNode) o;
        return Objects.equals(edges, that.edges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edges);
    }
}
