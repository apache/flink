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

import org.apache.flink.optimizer.dag.BinaryUnionNode;
import org.apache.flink.runtime.operators.DriverStrategy;

/** A special subclass for the union to make it identifiable. */
public class BinaryUnionPlanNode extends DualInputPlanNode {

    /** @param template */
    public BinaryUnionPlanNode(BinaryUnionNode template, Channel in1, Channel in2) {
        super(template, "Union", in1, in2, DriverStrategy.UNION);
    }

    public BinaryUnionPlanNode(BinaryUnionPlanNode toSwapFrom) {
        super(
                toSwapFrom.getOptimizerNode(),
                "Union-With-Cached",
                toSwapFrom.getInput2(),
                toSwapFrom.getInput1(),
                DriverStrategy.UNION_WITH_CACHED);

        this.globalProps = toSwapFrom.globalProps;
        this.localProps = toSwapFrom.localProps;
        this.nodeCosts = toSwapFrom.nodeCosts;
        this.cumulativeCosts = toSwapFrom.cumulativeCosts;

        setParallelism(toSwapFrom.getParallelism());
    }

    public BinaryUnionNode getOptimizerNode() {
        return (BinaryUnionNode) this.template;
    }

    public boolean unionsStaticAndDynamicPath() {
        return getInput1().isOnDynamicPath() != getInput2().isOnDynamicPath();
    }

    @Override
    public int getMemoryConsumerWeight() {
        return 0;
    }
}
