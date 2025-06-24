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

package org.apache.flink.table.utils;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.io.IOException;
import java.util.List;

/** Mocking {@link Planner} for tests. */
public class PlannerMock implements Planner {

    @Override
    public Parser getParser() {
        return new ParserMock();
    }

    @Override
    public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
        return null;
    }

    @Override
    public String explain(
            List<Operation> operations, ExplainFormat format, ExplainDetail... extraDetails) {
        return null;
    }

    @Override
    public InternalPlan loadPlan(PlanReference planReference) throws IOException {
        return null;
    }

    @Override
    public InternalPlan compilePlan(List<ModifyOperation> modifyOperations) {
        return null;
    }

    @Override
    public List<Transformation<?>> translatePlan(InternalPlan plan) {
        return null;
    }

    @Override
    public String explainPlan(InternalPlan plan, ExplainDetail... extraDetails) {
        return null;
    }
}
