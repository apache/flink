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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalValues;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalValues;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/** Rule that converts {@link FlinkLogicalValues} to {@link StreamPhysicalValues}. */
public class StreamPhysicalValuesRule extends ConverterRule {

    public static final RelOptRule INSTANCE =
            new StreamPhysicalValuesRule(
                    Config.INSTANCE
                            .withConversion(
                                    FlinkLogicalValues.class,
                                    FlinkConventions.LOGICAL(),
                                    FlinkConventions.STREAM_PHYSICAL(),
                                    "StreamPhysicalValuesRule")
                            .withRuleFactory(StreamPhysicalValuesRule::new));

    protected StreamPhysicalValuesRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        FlinkLogicalValues values = (FlinkLogicalValues) rel;
        RelTraitSet traitSet = rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());

        return new StreamPhysicalValues(
                rel.getCluster(), traitSet, values.getTuples(), rel.getRowType());
    }
}
