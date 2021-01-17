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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.runtime.operators.AbstractProcessStreamOperator;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexProgram;

/** Stream {@link ExecNode} for Calc. */
public class StreamExecCalc extends CommonExecCalc implements StreamExecNode<RowData> {

    public StreamExecCalc(
            RexProgram calcProgram, ExecEdge inputEdge, RowType outputType, String description) {
        super(
                calcProgram,
                AbstractProcessStreamOperator.class,
                true,
                inputEdge,
                outputType,
                description);
    }
}
