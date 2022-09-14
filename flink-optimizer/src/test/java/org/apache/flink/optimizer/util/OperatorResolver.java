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

package org.apache.flink.optimizer.util;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.base.BulkIterationBase;
import org.apache.flink.api.common.operators.base.DeltaIterationBase;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Utility to get operator instances from plans via name. */
public class OperatorResolver implements Visitor<Operator<?>> {

    private final Map<String, List<Operator<?>>> map;
    private Set<Operator<?>> seen;

    public OperatorResolver(Plan p) {
        this.map = new HashMap<String, List<Operator<?>>>();
        this.seen = new HashSet<Operator<?>>();

        p.accept(this);
        this.seen = null;
    }

    @SuppressWarnings("unchecked")
    public <T extends Operator<?>> T getNode(String name) {
        List<Operator<?>> nodes = this.map.get(name);
        if (nodes == null || nodes.isEmpty()) {
            throw new RuntimeException("No nodes found with the given name.");
        } else if (nodes.size() != 1) {
            throw new RuntimeException("Multiple nodes found with the given name.");
        } else {
            return (T) nodes.get(0);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Operator<?>> T getNode(String name, Class<? extends RichFunction> stubClass) {
        List<Operator<?>> nodes = this.map.get(name);
        if (nodes == null || nodes.isEmpty()) {
            throw new RuntimeException("No node found with the given name and stub class.");
        } else {
            Operator<?> found = null;
            for (Operator<?> node : nodes) {
                if (node.getClass() == stubClass) {
                    if (found == null) {
                        found = node;
                    } else {
                        throw new RuntimeException(
                                "Multiple nodes found with the given name and stub class.");
                    }
                }
            }
            if (found == null) {
                throw new RuntimeException("No node found with the given name and stub class.");
            } else {
                return (T) found;
            }
        }
    }

    public List<Operator<?>> getNodes(String name) {
        List<Operator<?>> nodes = this.map.get(name);
        if (nodes == null || nodes.isEmpty()) {
            throw new RuntimeException("No node found with the given name.");
        } else {
            return new ArrayList<Operator<?>>(nodes);
        }
    }

    @Override
    public boolean preVisit(Operator<?> visitable) {
        if (this.seen.add(visitable)) {
            // add to  the map
            final String name = visitable.getName();
            List<Operator<?>> list = this.map.get(name);
            if (list == null) {
                list = new ArrayList<Operator<?>>(2);
                this.map.put(name, list);
            }
            list.add(visitable);

            // recurse into bulk iterations
            if (visitable instanceof BulkIterationBase) {
                ((BulkIterationBase) visitable).getNextPartialSolution().accept(this);
            } else if (visitable instanceof DeltaIterationBase) {
                ((DeltaIterationBase) visitable).getSolutionSetDelta().accept(this);
                ((DeltaIterationBase) visitable).getNextWorkset().accept(this);
            }

            return true;
        } else {
            return false;
        }
    }

    @Override
    public void postVisit(Operator<?> visitable) {}
}
