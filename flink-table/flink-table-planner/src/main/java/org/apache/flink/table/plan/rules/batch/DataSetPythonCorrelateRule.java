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

package org.apache.flink.table.plan.rules.batch;

import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.dataset.DataSetPythonCorrelate;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.plan.rules.AbstractPythonCorrelateRuleBase;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import scala.Option;

/**
 * The physical rule is responsible for converting {@link FlinkLogicalCorrelate} to {@link
 * DataSetPythonCorrelate}.
 */
public class DataSetPythonCorrelateRule extends AbstractPythonCorrelateRuleBase {

    public static final DataSetPythonCorrelateRule INSTANCE = new DataSetPythonCorrelateRule();

    private DataSetPythonCorrelateRule() {
        super(FlinkConventions.DATASET(), "DataSetPythonCorrelateRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        DataSetPythonCorrelateFactory factory = new DataSetPythonCorrelateFactory(rel);
        return factory.convertToCorrelate();
    }

    /** The factory is responsible for creating {@link DataSetPythonCorrelate}. */
    private static class DataSetPythonCorrelateFactory extends PythonCorrelateFactoryBase {
        private DataSetPythonCorrelateFactory(RelNode rel) {
            super(rel, FlinkConventions.DATASET());
        }

        @Override
        public RelNode createPythonCorrelateNode(RelNode relNode, Option<RexNode> condition) {
            FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) relNode;
            return new DataSetPythonCorrelate(
                    relNode.getCluster(),
                    traitSet,
                    convInput,
                    scan,
                    condition,
                    correlateRel.getRowType(),
                    join.getRowType(),
                    join.getJoinType(),
                    "DataSetPythonCorrelateRule");
        }
    }
}
