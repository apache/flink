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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The base class for aggregate {@link ExecNode}. */
public abstract class StreamExecAggregateBase extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String FIELD_NAME_GROUPING = "grouping";
    public static final String FIELD_NAME_AGG_CALLS = "aggCalls";
    public static final String FIELD_NAME_AGG_CALL_NEED_RETRACTIONS = "aggCallNeedRetractions";
    public static final String FIELD_NAME_GENERATE_UPDATE_BEFORE = "generateUpdateBefore";
    public static final String FIELD_NAME_NEED_RETRACTION = "needRetraction";

    protected StreamExecAggregateBase(
            int id,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
    }
}
