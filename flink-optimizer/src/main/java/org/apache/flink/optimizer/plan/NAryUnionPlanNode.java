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

package org.apache.flink.optimizer.plan;

import org.apache.flink.optimizer.costs.Costs;
import org.apache.flink.optimizer.dag.BinaryUnionNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.util.IterableIterator;
import org.apache.flink.util.Visitor;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** A union operation over multiple inputs (2 or more). */
public class NAryUnionPlanNode extends PlanNode {

    private final List<Channel> inputs;

    /** @param template */
    public NAryUnionPlanNode(
            BinaryUnionNode template,
            List<Channel> inputs,
            GlobalProperties gProps,
            Costs cumulativeCosts) {
        super(template, "Union", DriverStrategy.NONE);

        this.inputs = inputs;
        this.globalProps = gProps;
        this.localProps = new LocalProperties();
        this.nodeCosts = new Costs();
        this.cumulativeCosts = cumulativeCosts;
    }

    @Override
    public void accept(Visitor<PlanNode> visitor) {
        visitor.preVisit(this);
        for (Channel c : this.inputs) {
            c.getSource().accept(visitor);
        }
        visitor.postVisit(this);
    }

    public List<Channel> getListOfInputs() {
        return this.inputs;
    }

    @Override
    public Iterable<Channel> getInputs() {
        return Collections.unmodifiableList(this.inputs);
    }

    @Override
    public Iterable<PlanNode> getPredecessors() {
        final Iterator<Channel> channels = this.inputs.iterator();
        return new IterableIterator<PlanNode>() {

            @Override
            public boolean hasNext() {
                return channels.hasNext();
            }

            @Override
            public PlanNode next() {
                return channels.next().getSource();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<PlanNode> iterator() {
                return this;
            }
        };
    }

    @Override
    public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
        // this node is used after the plan enumeration. consequently, this will never be invoked
        // here
        throw new UnsupportedOperationException();
    }
}
