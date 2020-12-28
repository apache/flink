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

package org.apache.flink.table.plan.rules.stream;

import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DataStreamPythonCorrelate;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.plan.rules.AbstractPythonCorrelateRuleBase;
import org.apache.flink.table.plan.schema.RowSchema;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import scala.Option;

/**
 * The physical rule is responsible for converting {@link FlinkLogicalCorrelate} to {@link
 * DataStreamPythonCorrelate}.
 */
public class DataStreamPythonCorrelateRule extends AbstractPythonCorrelateRuleBase {

    public static final RelOptRule INSTANCE = new DataStreamPythonCorrelateRule();

    private DataStreamPythonCorrelateRule() {
        super(FlinkConventions.DATASTREAM(), "DataStreamPythonCorrelateRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        DataStreamPythonCorrelateFactory factory = new DataStreamPythonCorrelateFactory(rel);
        return factory.convertToCorrelate();
    }

    /** The factory is responsible for creating {@link DataStreamPythonCorrelate}. */
    private static class DataStreamPythonCorrelateFactory extends PythonCorrelateFactoryBase {
        private DataStreamPythonCorrelateFactory(RelNode rel) {
            super(rel, FlinkConventions.DATASTREAM());
        }

        @Override
        public RelNode createPythonCorrelateNode(RelNode relNode, Option<RexNode> condition) {
            FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) relNode;
            return new DataStreamPythonCorrelate(
                    relNode.getCluster(),
                    traitSet,
                    new RowSchema(convInput.getRowType()),
                    convInput,
                    scan,
                    condition,
                    new RowSchema(correlateRel.getRowType()),
                    new RowSchema(join.getRowType()),
                    join.getJoinType(),
                    "DataStreamPythonCorrelateRule");
        }
    }
}
