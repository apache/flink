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

package org.apache.flink.graph;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * An Edge represents a link between two {@link Vertex vertices}, the source and the target and can
 * carry an attached value. For edges with no value, use {@link org.apache.flink.types.NullValue} as
 * the value type.
 *
 * @param <K> the key type for the sources and target vertices
 * @param <V> the edge value type
 */
public class Edge<K, V> extends Tuple3<K, K, V> {

    private static final long serialVersionUID = 1L;

    public Edge() {}

    public Edge(K source, K target, V value) {
        this.f0 = source;
        this.f1 = target;
        this.f2 = value;
    }

    /**
     * Reverses the direction of this Edge.
     *
     * @return a new Edge, where the source is the original Edge's target and the target is the
     *     original Edge's source.
     */
    public Edge<K, V> reverse() {
        return new Edge<>(this.f1, this.f0, this.f2);
    }

    public void setSource(K source) {
        this.f0 = source;
    }

    public K getSource() {
        return this.f0;
    }

    public void setTarget(K target) {
        this.f1 = target;
    }

    public K getTarget() {
        return f1;
    }

    public void setValue(V value) {
        this.f2 = value;
    }

    public V getValue() {
        return f2;
    }
}
