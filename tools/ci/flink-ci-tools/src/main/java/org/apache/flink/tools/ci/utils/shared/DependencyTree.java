/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tools.ci.utils.shared;

import org.apache.flink.annotation.VisibleForTesting;

import com.google.common.graph.Traverser;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents a dependency tree.
 *
 * <p>Every dependency can only occur exactly once.
 */
public class DependencyTree {

    private final Map<String, Node> lookup = new LinkedHashMap<>();
    private final List<Node> directDependencies = new ArrayList<>();

    public DependencyTree addDirectDependency(Dependency dependency) {
        final String key = getKey(dependency);
        if (lookup.containsKey(key)) {
            return this;
        }
        final Node node = new Node(dependency, null);

        lookup.put(key, node);
        directDependencies.add(node);

        return this;
    }

    public DependencyTree addTransitiveDependencyTo(
            Dependency transitiveDependency, Dependency parent) {
        final String key = getKey(transitiveDependency);
        if (lookup.containsKey(key)) {
            return this;
        }
        final Node node = lookup.get(getKey(parent)).addTransitiveDependency(transitiveDependency);

        lookup.put(key, node);

        return this;
    }

    private static final class Node {
        private final Dependency dependency;
        @Nullable private final Node parent;
        private final List<Node> children = new ArrayList<>();

        private Node(Dependency dependency, @Nullable Node parent) {
            this.dependency = dependency;
            this.parent = parent;
        }

        public Node addTransitiveDependency(Dependency dependency) {
            final Node node = new Node(dependency, this);
            this.children.add(node);
            return node;
        }

        private boolean isRoot() {
            return parent == null;
        }
    }

    public List<Dependency> getDirectDependencies() {
        return directDependencies.stream()
                .map(node -> node.dependency)
                .collect(Collectors.toList());
    }

    public List<Dependency> getPathTo(Dependency dependency) {
        final LinkedList<Dependency> path = new LinkedList<>();

        Node node = lookup.get(getKey(dependency));
        path.addFirst(node.dependency);
        while (!node.isRoot()) {
            node = node.parent;
            path.addFirst(node.dependency);
        }

        return path;
    }

    public Stream<Dependency> flatten() {
        return StreamSupport.stream(
                        Traverser.<Node>forTree(node -> node.children)
                                .depthFirstPreOrder(directDependencies)
                                .spliterator(),
                        false)
                .map(node -> node.dependency);
    }

    /**
     * We don't use the {@link Dependency} as a key because we don't want lookups to be dependent on
     * scope or the optional flag.
     *
     * @param dependency
     * @return
     */
    @VisibleForTesting
    static String getKey(Dependency dependency) {
        return dependency.getGroupId()
                + ":"
                + dependency.getArtifactId()
                + ":"
                + dependency.getVersion()
                + ":"
                + dependency.getClassifier().orElse("(no-classifier)");
    }
}
