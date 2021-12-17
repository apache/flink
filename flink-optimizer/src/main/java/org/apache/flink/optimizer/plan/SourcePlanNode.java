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

import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.optimizer.dag.DataSourceNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.util.Visitor;

import java.util.Collections;

import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

/** Plan candidate node for data flow sources that have no input and no special strategies. */
public class SourcePlanNode extends PlanNode {

    private TypeSerializerFactory<?> serializer;

    /**
     * Constructs a new source candidate node that uses <i>NONE</i> as its local strategy.
     *
     * @param template The template optimizer node that this candidate is created for.
     */
    public SourcePlanNode(DataSourceNode template, String nodeName) {
        this(template, nodeName, new GlobalProperties(), new LocalProperties());
    }

    public SourcePlanNode(
            DataSourceNode template,
            String nodeName,
            GlobalProperties gprops,
            LocalProperties lprops) {
        super(template, nodeName, DriverStrategy.NONE);

        this.globalProps = gprops;
        this.localProps = lprops;
        updatePropertiesWithUniqueSets(template.getUniqueFields());
    }

    // --------------------------------------------------------------------------------------------

    public DataSourceNode getDataSourceNode() {
        return (DataSourceNode) this.template;
    }

    /**
     * Gets the serializer from this PlanNode.
     *
     * @return The serializer.
     */
    public TypeSerializerFactory<?> getSerializer() {
        return serializer;
    }

    /**
     * Sets the serializer for this PlanNode.
     *
     * @param serializer The serializer to set.
     */
    public void setSerializer(TypeSerializerFactory<?> serializer) {
        this.serializer = serializer;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void accept(Visitor<PlanNode> visitor) {
        if (visitor.preVisit(this)) {
            visitor.postVisit(this);
        }
    }

    @Override
    public Iterable<PlanNode> getPredecessors() {
        return Collections.<PlanNode>emptyList();
    }

    @Override
    public Iterable<Channel> getInputs() {
        return Collections.<Channel>emptyList();
    }

    @Override
    public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
        if (source == this) {
            return FOUND_SOURCE;
        } else {
            return NOT_FOUND;
        }
    }
}
