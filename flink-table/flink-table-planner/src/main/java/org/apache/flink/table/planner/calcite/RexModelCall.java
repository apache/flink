/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.planner.plan.schema.ModelProviderModel;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;

import java.util.List;

/**
 * A {@link RexCall} that represents a call to a model provider model.
 *
 * <p>This is used to represent calls to models in the Flink SQL planner.
 */
public class RexModelCall extends RexCall {

    private ModelProviderModel modelProviderModel;
    private RelDataType inputType;

    public RexModelCall(
            ModelProviderModel modelProviderModel, RelDataType inputType, RelDataType outputType) {
        super(
                outputType,
                new SqlSpecialOperator("RexModelCall", SqlKind.OTHER_FUNCTION),
                List.of());
        this.modelProviderModel = modelProviderModel;
        this.inputType = inputType;
    }

    @Override
    public RelDataType getType() {
        return type;
    }

    @Override
    protected String computeDigest(boolean withType) {
        final StringBuilder sb = new StringBuilder(op.getName());
        sb.append("(");
        sb.append(modelProviderModel.getContextResolvedModel().getIdentifier());
        sb.append(")");
        return sb.toString();
    }
}
