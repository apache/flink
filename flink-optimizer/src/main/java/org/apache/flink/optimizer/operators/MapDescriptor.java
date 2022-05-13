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

package org.apache.flink.optimizer.operators;

import org.apache.flink.optimizer.dag.SingleInputNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;

import java.util.Collections;
import java.util.List;

public class MapDescriptor extends OperatorDescriptorSingle {

    @Override
    public DriverStrategy getStrategy() {
        return DriverStrategy.MAP;
    }

    @Override
    public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
        return new SingleInputPlanNode(
                node, "Map (" + node.getOperator().getName() + ")", in, DriverStrategy.MAP);
    }

    @Override
    protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
        RequestedGlobalProperties rgp = new RequestedGlobalProperties();
        rgp.setAnyDistribution();
        return Collections.singletonList(rgp);
    }

    @Override
    protected List<RequestedLocalProperties> createPossibleLocalProperties() {
        return Collections.singletonList(new RequestedLocalProperties());
    }

    @Override
    public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
        return gProps;
    }

    @Override
    public LocalProperties computeLocalProperties(LocalProperties lProps) {
        return lProps;
    }
}
